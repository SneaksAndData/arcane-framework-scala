package com.sneaksanddata.arcane.framework
package services.mssql.base

import logging.ZIOLogAnnotations.zlogStream
import models.schemas.{ArcaneSchema, DataRow, given_CanAdd_ArcaneSchema}
import services.base.SchemaProvider
import services.mssql.QueryProvider.{getBackfillQuery, getChangesQuery, getSchemaQuery}
import services.mssql.SqlSchema.toSchema
import services.mssql.base.MsSqlReader.{closeSafe, executeQuerySafe}
import services.mssql.base.{CanPeekHead, MsSqlServerFieldsFilteringService, QueryResult}
import services.mssql.query.LazyQueryResult.toDataRow
import services.mssql.query.{LazyQueryResult, ScalarQueryResult}
import services.mssql.{MsSqlVersionedBatch, QueryProvider, SqlSchema, given_Conversion_SqlDataRow_DataRow}

import com.microsoft.sqlserver.jdbc.SQLServerDriver
import zio.stream.ZStream
import zio.{Scope, Task, UIO, ZIO, ZLayer}

import java.sql.{Connection, ResultSet, Statement}
import java.time.Duration
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.annotation.tailrec

/** Represents a summary of a column in a table. The first element is the name of the column, and the second element is
  * true if the column is a primary key.
  */
type ColumnSummary = (String, Boolean)

/** Represents a query to be executed on a Microsoft SQL Server database.
  */
type MsSqlQuery = String

/** Represents the connection options for a Microsoft SQL Server database.
  *
  * @param connectionUrl
  *   The connection URL for the database.
  * @param schemaName
  *   The name of the schema.
  * @param tableName
  *   The name of the table.
  */
case class ConnectionOptions(connectionUrl: String, schemaName: String, tableName: String, fetchSize: Option[Int])

/** Represents a connection to a Microsoft SQL Server database.
  *
  * @param connectionOptions
  *   The connection options for the database.
  */
class MsSqlReader(
    val connectionOptions: ConnectionOptions,
    fieldsFilteringService: MsSqlServerFieldsFilteringService
) extends AutoCloseable
    with SchemaProvider[ArcaneSchema]:
  lazy val catalog: String = connection.getCatalog

  private val driver          = new SQLServerDriver()
  private lazy val connection = driver.connect(connectionOptions.connectionUrl, new Properties())
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  private implicit val formatter: DateTimeFormatter          = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  /** Gets the column summaries for the table in the database.
    *
    * @return
    *   An effect containing the column summaries for the table in the database.
    */
  def getColumnSummaries: Task[List[ColumnSummary]] =
    for
      query <- QueryProvider.getColumnSummariesQuery(connectionOptions.schemaName, connectionOptions.tableName, catalog)
      result <- executeColumnSummariesQuery(query)
    yield result

  /** Run a backfill query on the database.
    *
    * @return
    *   A stream containing the result of a backfill query.
    */
  def backfill: ZStream[Any, Throwable, DataRow] =
    for
      query     <- ZStream.fromZIO(this.getBackfillQuery)
      statement <- ZStream.acquireReleaseWith(ZIO.attempt(connection.createStatement()))(st => ZIO.succeed(st.close()))
      resultSet <- ZStream.acquireReleaseWith(ZIO.attempt(statement.executeQuery(query)))(rs => rs.closeSafe(statement))
      _         <- zlogStream("Acquired result set with fetch size %s", resultSet.getFetchSize.toString)
      _         <- ZStream.succeed(resultSet.setFetchSize(connectionOptions.fetchSize.getOrElse(1000)))
      _         <- zlogStream("Updated result set fetch size to %s", resultSet.getFetchSize.toString)
      stream <- ZStream.unfoldZIO(resultSet.next()) { hasNext =>
        if hasNext then
          for
            columns    <- ZIO.attemptBlockingInterrupt(resultSet.getMetaData.getColumnCount)
            row        <- ZIO.fromTry(toDataRow(resultSet, columns, List.empty))
            hasNextRow <- ZIO.attemptBlockingInterrupt(resultSet.next())
          yield Some((row, hasNextRow))
        else ZIO.succeed(None)
      }
    yield stream

  /** Gets the changes in the database since the given version.
    * @param maybeLatestVersion
    *   The version to start from.
    * @param lookBackInterval
    *   The look back interval for the query.
    * @return
    *   An effect containing the changes in the database since the given version and the latest observed version.
    */
  def getChanges(latestVersion: Long): Task[MsSqlVersionedBatch] =
    val query = QueryProvider.getChangeTrackingVersionQuery(maybeLatestVersion, lookBackInterval)
    ZIO.scoped {
      for
        versionResult <- ZIO.fromAutoCloseable(
          executeQuery(query, connection, (st, rs) => ScalarQueryResult.apply(st, rs, readChangeTrackingVersion))
        )
        version = versionResult.read.getOrElse(Long.MaxValue)
        changesQuery <- this.getChangesQuery(version - 1)

        // We don't need to close the statement/result set here, since the ownership is passed to the LazyQueryResult
        // And the LazyQueryResult will close the statement/result set when it is closed.
        result <- executeQuery(changesQuery, connection, LazyQueryResult.apply)
      yield MsSqlReader.ensureHead((result, version))
    }

  private def readChangeTrackingVersion(resultSet: ResultSet): Option[Long] =
    resultSet.getMetaData.getColumnType(1) match
      case java.sql.Types.BIGINT => Option(resultSet.getObject(1)).flatMap(v => Some(v.asInstanceOf[Long]))
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid column type for change tracking version: ${resultSet.getMetaData.getColumnType(1)}, expected BIGINT"
        )

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
      query        <- this.getSchemaQuery
      sqlSchema    <- getSqlSchema(query)
      arcaneSchema <- ZIO.fromTry(toSchema(sqlSchema, empty))
    yield arcaneSchema

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

object MsSqlReader:

  type Environment = ConnectionOptions & MsSqlServerFieldsFilteringService

  /** Creates a new Microsoft SQL Server connection.
    *
    * @param connectionOptions
    *   The connection options for the database.
    * @param fieldsFilteringService
    *   The service that filters the fields in queries.
    * @return
    *   A new Microsoft SQL Server connection.
    */
  def apply(
      connectionOptions: ConnectionOptions,
      fieldsFilteringService: MsSqlServerFieldsFilteringService
  ): MsSqlReader =
    new MsSqlReader(connectionOptions, fieldsFilteringService)

  /** The ZLayer that creates the MsSqlDataProvider.
    */
  val layer: ZLayer[Environment, Nothing, MsSqlReader & SchemaProvider[ArcaneSchema]] =
    ZLayer.scoped {
      ZIO.fromAutoCloseable {
        for
          connectionOptions      <- ZIO.service[ConnectionOptions]
          fieldsFilteringService <- ZIO.service[MsSqlServerFieldsFilteringService]
        yield MsSqlReader(connectionOptions, fieldsFilteringService)
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
    * @param resultSet
    *   The result set to close.
    * @param statement
    *   The statement to close.
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
    * MUST BE CLOSED THIS WAY
    * @param resultSet
    *   The result set to close.
    * @param statement
    *   The statement to close.
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
  private def ensureHead(result: MsSqlVersionedBatch): MsSqlVersionedBatch =
    val (queryResult, version) = result
    (queryResult.peekHead, version)
