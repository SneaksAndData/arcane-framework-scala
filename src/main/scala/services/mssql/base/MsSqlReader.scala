package com.sneaksanddata.arcane.framework
package services.mssql.base

import logging.ZIOLogAnnotations.{zlog, zlogStream}
import models.app.PluginStreamContext
import models.schemas.{ArcaneSchema, DataRow, given_CanAdd_ArcaneSchema}
import models.settings.mssql.MsSqlServerDatabaseSourceSettings
import services.base.SchemaProvider
import services.mssql.QueryProvider.{getBackfillQuery, getChangesQuery, getSchemaQuery}
import services.mssql.given_Conversion_SqlSchema_ArcaneSchema
import services.mssql.base.MsSqlReader.{closeSafe, executeQuerySafe}
import services.mssql.query.LazyQueryResult.toDataRow
import services.mssql.query.{LazyQueryResult, ScalarQueryResult}
import services.mssql.versioning.MsSqlWatermark
import services.mssql.*
import services.mssql.given_Conversion_SqlDataRow_DataRow
import services.streaming.base.StructuredZStream

import com.microsoft.sqlserver.jdbc.SQLServerDriver
import com.sneaksanddata.arcane.framework.models.settings.TableNaming.getBackfillTableName
import com.sneaksanddata.arcane.framework.models.sharding.{BootstrappedShard, DefaultBootstrappedShard}
import zio.stream.ZStream
import zio.{Scope, Task, UIO, ZIO, ZLayer}

import java.sql.{Connection, ResultSet, Statement}
import java.time.format.DateTimeFormatter
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.Properties
import scala.annotation.tailrec

/** Represents a summary of a column in a table. The first element is the name of the column, and the second element is
  * true if the column is a primary key.
  */
type ColumnSummary = (String, Boolean)

/** Represents a query to be executed on a Microsoft SQL Server database.
  */
type MsSqlQuery = String

/** Represents a connection to a Microsoft SQL Server database.
  *
  * @param connectionSettings
  *   The connection options for the database.
  */
class MsSqlReader(
    val connectionSettings: MsSqlServerDatabaseSourceSettings,
    fieldsFilteringService: MsSqlServerFieldsFilteringService
) extends AutoCloseable
    with SchemaProvider[ArcaneSchema]:

  lazy val catalog: String    = connection.getCatalog
  private val shardingParallelism = Runtime.getRuntime.availableProcessors() * 2
  private val driver          = new SQLServerDriver()
  private lazy val connection = driver.connect(connectionSettings.getConnectionString, new Properties())
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  implicit val formatter: DateTimeFormatter                  = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  /** Gets the column summaries for the table in the database.
    *
    * @return
    *   An effect containing the column summaries for the table in the database.
    */
  def getColumnSummaries(schemaName: String, tableName: String): Task[List[ColumnSummary]] =
    for
      query <- QueryProvider.getColumnSummariesQuery(
        connectionSettings.schemaName,
        connectionSettings.tableName,
        catalog
      )
      result <- executeColumnSummariesQuery(query)
    yield result

  /**
   * Create a stream from a provided shard table.
    */
  def createShardStream(shardTableName: String): Task[StructuredZStream] = getSchema.map { schema =>
    (
      for
        query <- ZStream.fromZIO(this.getBackfillQuery(shardTableName))
        statement <- ZStream
          .acquireReleaseWith(ZIO.attempt(connection.createStatement()))(st => ZIO.succeed(st.close()))
        resultSet <- ZStream
          .acquireReleaseWith(ZIO.attempt(statement.executeQuery(query)))(rs => rs.closeSafe(statement))
        _ <- zlogStream("Acquired shard %s result set with fetch size %s", shardTableName, resultSet.getFetchSize.toString)
        _ <- ZStream.succeed(resultSet.setFetchSize(connectionSettings.fetchSize.getOrElse(1000)))
        _ <- zlogStream("Updated shard %s result set fetch size to %s", shardTableName, resultSet.getFetchSize.toString)
        stream <- ZStream.unfoldZIO(resultSet.next()) { hasNext =>
          if hasNext then
            for
              columns    <- ZIO.attemptBlockingInterrupt(resultSet.getMetaData.getColumnCount)
              row        <- ZIO.fromTry(toDataRow(resultSet, columns, List.empty))
              hasNextRow <- ZIO.attemptBlockingInterrupt(resultSet.next())
            yield Some((row.handleSpecialTypes, hasNextRow))
          else ZIO.succeed(None)
        }
      yield stream,
      schema
    )
  }

  private def createShardTable(shardNumber: Int, totalShards: Int, backfillId: String, summaries: List[ColumnSummary]): Task[String] = for
    tableName <- ZIO.succeed(s"${backfillId}__shard_$shardNumber")
    _ <- zlog("Creating a table %s for shard #%s", tableName, shardNumber.toString)
    _ <- ZIO.acquireReleaseWith(ZIO.attempt(connection.createStatement()))(st => ZIO.succeed(st.close())) { statement =>
      ZIO.attempt(statement.execute(QueryProvider.getCreateCloneQuery(connectionSettings.schemaName, connectionSettings.tableName, connectionSettings.backfillShardSchemaName, tableName)))
    }
    _ <- zlog("Filling shard table %s", tableName)
    _ <- ZIO.acquireReleaseWith(ZIO.attempt(connection.createStatement()))(st => ZIO.succeed(st.close())) { statement =>
      ZIO.attempt(statement.execute(QueryProvider.getFillShardQuery(connectionSettings.schemaName, connectionSettings.tableName, connectionSettings.backfillShardSchemaName, tableName, QueryProvider.getMergeExpression(summaries, "tq"), totalShards, shardNumber)))
    }
    _ <- zlog("Preparing shard table %s for streaming", tableName)
    // add PK
    _ <- ZIO.acquireReleaseWith(ZIO.attempt(connection.createStatement()))(st => ZIO.succeed(st.close())) { statement =>
      ZIO.attempt(statement.execute(QueryProvider.getCreatePrimaryKeyQuery(connectionSettings.backfillShardSchemaName, tableName, summaries)))
    }
    // enable Change Tracking
    _ <- ZIO.acquireReleaseWith(ZIO.attempt(connection.createStatement()))(st => ZIO.succeed(st.close())) { statement =>
      ZIO.attempt(statement.execute(s"ALTER TABLE [${connectionSettings.backfillShardSchemaName}.[$tableName] ENABLE CHANGE_TRACKING"))
    }
  yield tableName


  /**
   * Evaluate shard count and prep shard tables on source side. Emits table names.
   */
  def prepareShardTables(backfillId: String, advisedShardSizeMib: Option[Int]): ZStream[Any, Throwable, String] = ZStream.fromZIO(for
    columnSummaries <- getColumnSummaries(connectionSettings.schemaName, connectionSettings.tableName)
    // first take estimate of a `select * from` query cost
    totalReadCost <- ZIO.acquireReleaseWith(ZIO.attempt(connection.createStatement()))(st => ZIO.succeed(st.close())) { statement =>
      ZIO.acquireReleaseWith(ZIO.attempt(statement.executeQuery(QueryProvider.getStatsProfileQuery(connectionSettings.schemaName, connectionSettings.tableName))))(rs => rs.closeSafe(statement)) { rs =>
        ZStream.unfold(rs.next()) { hasNext =>
          if hasNext then
            Some(Option(rs.getDouble("EstimateIO")).getOrElse(0.0) + Option(rs.getDouble("EstimateCPU")).getOrElse(0.0), rs.next())
          else
            None
        }.runSum
      }
    }
    shardProfile <- ZIO.acquireReleaseWith(ZIO.attempt(connection.createStatement()))(st => ZIO.succeed(st.close())) { statement =>
      val profileQuery = advisedShardSizeMib match
        case Some(advisedSize) => QueryProvider.getSourcePhysicalStatsQuery(connectionSettings.schemaName, connectionSettings.tableName, advisedSize)
        case None => QueryProvider.getSourcePhysicalStatsQuery(connectionSettings.schemaName, connectionSettings.tableName, totalReadCost)

      ZIO.acquireReleaseWith(ZIO.attempt(statement.executeQuery(profileQuery)))(rs => rs.closeSafe(statement)) { rs =>
        if rs.next() then
          ZIO.succeed((sizeGib = rs.getDouble(1), shardCount = rs.getInt(2), recordsPerShard = rs.getLong(3), summaries = columnSummaries))
        else
          ZIO.fail(new Throwable(s"Unable to determine row count for source entity ${connectionSettings.schemaName}.${connectionSettings.tableName}"))
      }
    }
    _ <- zlog(s"Created a shard profile for the backfill: table of size %s will be split into %s shards, with %s rows/shard", shardProfile.sizeGib.toString, shardProfile.shardCount.toString, shardProfile.recordsPerShard.toString)
  yield shardProfile).flatMap { profile => ZStream
    .fromIterable(1 to profile.shardCount)
    .mapZIOPar(shardingParallelism)(id => createShardTable(id, profile.shardCount, backfillId, profile.summaries))
  }

  private def unfoldBatch[T <: AutoCloseable & QueryResult[Iterator[DataRow]]](
      batch: T
  ): ZStream[Any, Throwable, DataRow] =
    for
      data     <- ZStream.acquireReleaseWith(ZIO.succeed(batch))(b => ZIO.succeed(b.close()))
      rowsList <- ZStream.fromZIO(ZIO.attemptBlocking(data.read))
      row      <- ZStream.fromIterator(rowsList, 1)
    yield row

  /** Gets the changes in the database since the given version.
    * @param latestVersion
    *   The version to fetch changes from.
    * @return
    *   An effect containing the changes in the database since the given version and the latest observed version.
    */
  def getChanges(latestVersion: MsSqlWatermark): ZStream[Any, Throwable, StructuredZStream] =
    ZStream.fromZIO(getSchema).map { schema =>
      (
        ZStream
          .fromZIO(ZIO.scoped {
            for
              changesQuery <- this.getChangesQuery(latestVersion - 1)

              // We don't need to close the statement/result set here, since the ownership is passed to the LazyQueryResult
              // And the LazyQueryResult will close the statement/result set when it is closed.
              result <- executeQuery(changesQuery, connection, LazyQueryResult.apply)
            yield MsSqlReader.ensureHead(result)
          })
          .flatMap(batch => unfoldBatch(batch))
          .map(_.handleSpecialTypes),
        schema
      )
    }

  def hasChanges(latestVersion: MsSqlWatermark): Task[Boolean] =
    ZIO.scoped {
      for
        changesQuery <- this.getChangesQuery(latestVersion - 1)

        // We don't need to close the statement/result set here, since the ownership is passed to the LazyQueryResult
        // And the LazyQueryResult will close the statement/result set when it is closed.
        result <- isEmptyQuery(changesQuery, connection)
      yield result
    }

  def getVersion(query: String): Task[Option[Long]] =
    def readChangeTrackingVersion(resultSet: ResultSet): Option[Long] =
      resultSet.getMetaData.getColumnType(1) match
        case java.sql.Types.BIGINT => Option(resultSet.getObject(1)).flatMap(v => Some(v.asInstanceOf[Long]))
        case _ =>
          throw new IllegalArgumentException(
            s"Invalid column type for change tracking version: ${resultSet.getMetaData.getColumnType(1)}, expected BIGINT"
          )

    ZIO.scoped {
      for
        _ <- zlog("Fetching version using query: %s", query)
        versionResult <- ZIO.fromAutoCloseable(
          executeQuery(query, connection, (st, rs) => ScalarQueryResult.apply(st, rs, readChangeTrackingVersion))
        )
        maybeVersion <- ZIO.attemptBlocking(versionResult.read)
      yield maybeVersion
    }

  def getVersionCommitTime(version: Long): Task[OffsetDateTime] =
    def readTime(resultSet: ResultSet): Option[OffsetDateTime] =
      resultSet.getMetaData.getColumnType(1) match
        case java.sql.Types.TIMESTAMP =>
          Option(resultSet.getTimestamp(1)).flatMap(v =>
            Some(OffsetDateTime.ofInstant(Instant.ofEpochMilli(v.getTime), ZoneOffset.UTC))
          )
        case _ =>
          throw new IllegalArgumentException(
            s"Invalid column type for change tracking version: ${resultSet.getMetaData.getColumnType(1)}, expected TIMESTAMP"
          )

    ZIO.scoped {
      for
        callTime <- ZIO.succeed(OffsetDateTime.now())
        query    <- ZIO.succeed(QueryProvider.getVersionCommitTime(version))
        _ <- zlog(
          "Fetching version commit time using query: %s. Will default to now() if this version is not fully accounted for yet",
          query
        )
        versionResult <- ZIO.fromAutoCloseable(
          executeQuery(query, connection, (st, rs) => ScalarQueryResult.apply(st, rs, readTime))
        )
        result <- ZIO.attemptBlocking(versionResult.read)
      // since sys.dm_tran_commit_table can be behind latest version, we fallback to call time to avoid reporting huge delays
      yield result.getOrElse(callTime)
    }

  def getCurrentVersion: ZIO[Any, Throwable, MsSqlWatermark] = for
    // get current version from CHANGE_TRACKING_CURRENT_VERSION() and the commit time associated with it
    version <- getVersion(QueryProvider.getCurrentVersionQuery).flatMap(ZIO.getOrFailWith(new Throwable("Unable to determine latest changeset version")))
    commitTime <- getVersionCommitTime(version)
  yield MsSqlWatermark.fromChangeTrackingVersion(version, commitTime)

  def timestampToVersion(timestamp: OffsetDateTime): Task[MsSqlWatermark] = for
    version <- getVersion(QueryProvider.getVersionFromTimestampQuery(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))).flatMap(ZIO.getOrFailWith(new Throwable(s"Unable to determine the changeset version matching timestamp ${timestamp.toString}")))
    commitTime <- getVersionCommitTime(version)
  yield MsSqlWatermark.fromChangeTrackingVersion(version, commitTime)

  /** Closes the connection to the database.
    */
  override def close(): Unit = connection.close()

  /** Gets an empty schema.
    *
    * @return
    *   An empty schema.
    */
  override def empty: this.SchemaType = ArcaneSchema.empty()

  /** Gets the schema for the data produced by Arcane. Implementation in the MsSqlConnection memorizes the schema since
    * we need to run an SQL query to get the schema.
    *
    * @return
    *   An effect containing the schema for the data produced by Arcane.
    */
  override lazy val getSchema: Task[this.SchemaType] =
    for
      query     <- this.getSchemaQuery
      sqlSchema <- getSqlSchema(query)
    yield sqlSchema

  private def getSqlSchema(query: String): Task[SqlSchema] =
    ZIO.scoped {
      for
        statement <- ZIO.fromAutoCloseable(ZIO.attemptBlocking(connection.createStatement()))
        resultSet <- statement.executeQuerySafe(query)
        metadata = resultSet.getMetaData
        columns <- ZIO.attemptBlocking {
          for i <- 1 to metadata.getColumnCount
          yield (metadata.getColumnName(i), metadata.getColumnType(i), metadata.getPrecision(i), metadata.getScale(i))
        }
      yield columns
    }

  private def executeColumnSummariesQuery(query: String): Task[List[ColumnSummary]] =
    ZIO.scoped {
      for
        statement <- ZIO.fromAutoCloseable(ZIO.attemptBlocking(connection.createStatement()))
        resultSet <- statement.executeQuerySafe(query)
        result    <- ZIO.fromTry(fieldsFilteringService.filter(readColumns(resultSet, List.empty)))
      yield result
    }

  @tailrec
  private def readColumns(resultSet: ResultSet, result: List[ColumnSummary]): List[ColumnSummary] =
    val hasNext = resultSet.next()

    if !hasNext then return result
    readColumns(resultSet, result ++ List((resultSet.getString(1), resultSet.getInt(2) == 1)))

  private type ResultFactory[QueryResultType] = (Statement, ResultSet) => QueryResultType

  private def executeQuery[QueryResultType](
      query: MsSqlQuery,
      connection: Connection,
      resultFactory: ResultFactory[QueryResultType]
  ): Task[QueryResultType] =
    for
      statement <- ZIO.attemptBlocking(connection.createStatement())
      resultSet <- ZIO.attemptBlocking(statement.executeQuery(query))
    yield resultFactory(statement, resultSet)

  private def isEmptyQuery(query: MsSqlQuery, connection: Connection): Task[Boolean] =
    for
      statement <- ZIO.attemptBlocking(connection.createStatement())
      resultSet <- ZIO.attemptBlocking(statement.executeQuery(query.replaceFirst("SELECT", "SELECT TOP 1")))
    yield resultSet.next()

object MsSqlReader:

  type Environment               = PluginStreamContext & MsSqlServerFieldsFilteringService
  private type SettingsExtractor = PluginStreamContext => MsSqlServerDatabaseSourceSettings

  /** Creates a new Microsoft SQL Server connection.
    *
    * @param connectionSettings
    *   The connection options for the database.
    * @param fieldsFilteringService
    *   The service that filters the fields in queries.
    * @return
    *   A new Microsoft SQL Server connection.
    */
  def apply(
      connectionSettings: MsSqlServerDatabaseSourceSettings,
      fieldsFilteringService: MsSqlServerFieldsFilteringService
  ): MsSqlReader =
    new MsSqlReader(connectionSettings, fieldsFilteringService)

  /** The ZLayer that creates the MsSqlDataProvider.
    */
  def getLayer(extractor: SettingsExtractor): ZLayer[Environment, Nothing, MsSqlReader & SchemaProvider[ArcaneSchema]] =
    ZLayer.scoped {
      ZIO.fromAutoCloseable {
        for
          context                <- ZIO.service[PluginStreamContext]
          fieldsFilteringService <- ZIO.service[MsSqlServerFieldsFilteringService]
        yield MsSqlReader(extractor(context), fieldsFilteringService)
      }
    }

  /** Represents a batch of data that can be used to backfill the data. Since the data is not versioned, the version is
    * always 0, and we don't need to be able to peek the head of the result.
    */
  type BackfillBatch = QueryResult[LazyQueryResult.OutputType]

  /** Closes the result in a safe way. MsSQL JDBC driver enforces the result set to iterate over all the rows returned
    * by the query if the result set is being closed without cancelling the statement first. see:
    * https://github.com/microsoft/mssql-jdbc/issues/877 for details. ALL RESULT SETS CREATED FROM MS SQL CONNECTION
    * MUST BE CLOSED THIS WAY
    * @return
    *   Scoped effect that tracks the result set and closes it when the effect is completed.
    */
  extension (statement: Statement)
    def executeQuerySafe(query: String): ZIO[Scope, Throwable, ResultSet] =
      for resultSet <- ZIO.acquireRelease(ZIO.attemptBlocking(statement.executeQuery(query)))(rs =>
          rs.closeSafe(statement)
        )
      yield resultSet

  /** Closes the result in a safe way. MsSQL JDBC driver enforces the result set to iterate over all the rows returned
    * by the query if the result set is being closed without cancelling the statement first. see:
    * https://github.com/microsoft/mssql-jdbc/issues/877 for details. ALL RESULT SETS CREATED FROM MS SQL CONNECTION
    * MUST BE CLOSED THIS WAY The statement to close.
    * @return
    *   UIO[Unit] that completes when the result set is closed.
    */
  extension (resultSet: ResultSet)
    def closeSafe(statement: Statement): UIO[Unit] =
      for
        _ <- ZIO.succeed(statement.cancel())
        _ <- ZIO.succeed(resultSet.close())
      yield ()

  /** Ensures that the head of the result (if any) saved and cannot be lost This is required to let the head function
    * work properly.
    */
  private def ensureHead(result: MsSqlQueryResult): MsSqlQueryResult = result.peekHead
