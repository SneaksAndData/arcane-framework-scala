package com.sneaksanddata.arcane.framework
package services.mssql

import models.{ArcaneSchema, ArcaneType, given_CanAdd_ArcaneSchema}
import services.base.SchemaProvider
import services.mssql.MsSqlConnection.{BackfillBatch, VersionedBatch}
import services.mssql.QueryProvider.{getBackfillQuery, getChangesQuery, getSchemaQuery}
import services.mssql.base.{CanPeekHead, QueryResult}
import services.mssql.query.{LazyQueryResult, ScalarQueryResult}
import utils.SqlUtils.toArcaneType

import com.microsoft.sqlserver.jdbc.SQLServerDriver
import zio.{Task, ZIO, ZLayer}

import java.sql.{Connection, ResultSet, Statement}
import java.time.Duration
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.annotation.tailrec
import scala.concurrent.{Future, blocking}
import scala.util.{Failure, Success, Try, Using}

/**
 * Represents a summary of a column in a table.
 * The first element is the name of the column, and the second element is true if the column is a primary key.
 */
type ColumnSummary = (String, Boolean)

/**
 * Represents a query to be executed on a Microsoft SQL Server database.
 */
type MsSqlQuery = String

/**
 * Represents the schema of a table in a Microsoft SQL Server database.
 * The schema is represented as a sequence of tuples, where each tuple contains
 * the column name, type (java.sql.Types), precision, and scale.
 */
type SqlSchema = Seq[(String, Int, Int, Int)]

/**
 * Represents the connection options for a Microsoft SQL Server database.
 *
 * @param connectionUrl       The connection URL for the database.
 * @param databaseName        The name of the database.
 * @param schemaName          The name of the schema.
 * @param tableName           The name of the table.
 * @param partitionExpression The partition expression for the table.
 */
case class ConnectionOptions(connectionUrl: String,
                             schemaName: String,
                             tableName: String,
                             partitionExpression: Option[String])

/**
 * Represents a connection to a Microsoft SQL Server database.
 *
 * @param connectionOptions The connection options for the database.
 */
class MsSqlConnection(val connectionOptions: ConnectionOptions) extends AutoCloseable with SchemaProvider[ArcaneSchema]:
  lazy val catalog: String = connection.getCatalog
  
  private val driver = new SQLServerDriver()
  private lazy val connection = driver.connect(connectionOptions.connectionUrl, new Properties())
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  private implicit val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  /**
   * Gets the column summaries for the table in the database.
   *
   * @return A future containing the column summaries for the table in the database.
   */
  def getColumnSummaries: Future[List[ColumnSummary]] =
    val tryQuery = QueryProvider.getColumnSummariesQuery(connectionOptions.schemaName, connectionOptions.tableName, catalog)
    for query <- Future.fromTry(tryQuery)
        result <- executeColumnSummariesQuery(query)
    yield result

  /**
   * Run a backfill query on the database.
   *
   * @return A future containing the result of the backfill.
   */
  def backfill: Future[BackfillBatch] =
    for query <- this.getBackfillQuery
        result <- executeQuery(query, connection, LazyQueryResult.apply)
    yield result

  /**
   * Gets the changes in the database since the given version.
   * @param maybeLatestVersion The version to start from.
   * @param lookBackInterval The look back interval for the query.
   * @return A future containing the changes in the database since the given version and the latest observed version.
   */
  def getChanges(maybeLatestVersion: Option[Long], lookBackInterval: Duration): Future[VersionedBatch] =
    val query = QueryProvider.getChangeTrackingVersionQuery(catalog, maybeLatestVersion, lookBackInterval)

    for versionResult <- executeQuery(query, connection, (st, rs) => ScalarQueryResult.apply(st, rs, readChangeTrackingVersion))
        version = versionResult.read.getOrElse(Long.MaxValue)
        changesQuery <- this.getChangesQuery(version - 1)
        result <- executeQuery(changesQuery, connection, LazyQueryResult.apply)
    yield MsSqlConnection.ensureHead((result, maybeLatestVersion.getOrElse(0)))

  private def readChangeTrackingVersion(resultSet: ResultSet): Option[Long] =
    resultSet.getMetaData.getColumnType(1) match
      case java.sql.Types.BIGINT => Option(resultSet.getObject(1)).flatMap(v => Some(v.asInstanceOf[Long]))
      case _ => throw new IllegalArgumentException(s"Invalid column type for change tracking version: ${resultSet.getMetaData.getColumnType(1)}, expected BIGINT")

  /**
   * Closes the connection to the database.
   */
  override def close(): Unit = connection.close()

  /**
   * Gets an empty schema.
   *
   * @return An empty schema.
   */
  override def empty: this.SchemaType = ArcaneSchema.empty()

  /**
   * Gets the schema for the data produced by Arcane.
   * Implementation in the MsSqlConnection memorizes the schema since we need to run an SQL query to 
   * get the schema.
   *
   * @return A future containing the schema for the data produced by Arcane.
   */
  override lazy val getSchema: Task[this.SchemaType] = ZIO.fromFuture( implicit ec => readSchemaFromSource)

  /**
   * Gets the schema for the data produced by Arcane.
   *
   * @return A future containing the schema for the data produced by Arcane.
   */
  private def readSchemaFromSource: Future[this.SchemaType] =
    for query <- this.getSchemaQuery
        sqlSchema <- getSqlSchema(query)
    yield toSchema(sqlSchema, empty) match
      case Success(schema) => schema
      case Failure(exception) => throw exception

  private def getSqlSchema(query: String): Future[SqlSchema] = Future {
    val columns = Using.Manager { use =>
      val statement = use(connection.createStatement())
      val resultSet = blocking {
        use(statement.executeQuery(query))
      }
      val metadata = resultSet.getMetaData
      for i <- 1 to metadata.getColumnCount yield (metadata.getColumnName(i),
        metadata.getColumnType(i),
        metadata.getPrecision(i),
        metadata.getScale(i))
    }
    columns.get
  }

  @tailrec
  private def toSchema(sqlSchema: SqlSchema, schema: this.SchemaType): Try[this.SchemaType] =
    sqlSchema match
      case Nil => Success(schema)
      case (name, fieldType, precision, scale) +: xs =>
        toArcaneType(fieldType, precision, scale) match
          case Success(arcaneType) => toSchema(xs, schema.addField(name, arcaneType))
          case Failure(exception) => Failure[this.SchemaType](exception)

  private def executeColumnSummariesQuery(query: String): Future[List[ColumnSummary]] =
    Future {
      val result = Using.Manager { use =>
        val statement = use(connection.createStatement())
        val resultSet = use(statement.executeQuery(query))
        blocking {
          readColumns(resultSet, List.empty)
        }
      }
      result.get
    }
    

  @tailrec
  private def readColumns(resultSet: ResultSet, result: List[ColumnSummary]): List[ColumnSummary] =
    val hasNext = resultSet.next()

    if !hasNext then
      return result
    readColumns(resultSet, result ++ List((resultSet.getString(1), resultSet.getInt(2) == 1)))

  private type ResultFactory[QueryResultType] = (Statement, ResultSet) => QueryResultType

  private def executeQuery[QueryResultType](query: MsSqlQuery, connection: Connection, resultFactory: ResultFactory[QueryResultType]): Future[QueryResultType] =
    Future {
      val statement = connection.createStatement()
      val resultSet = blocking {
        statement.executeQuery(query)
      }
      resultFactory(statement, resultSet)
    }

object MsSqlConnection:
  /**
   * Creates a new Microsoft SQL Server connection.
   *
   * @param connectionOptions The connection options for the database.
   * @return A new Microsoft SQL Server connection.
   */
  def apply(connectionOptions: ConnectionOptions): MsSqlConnection = new MsSqlConnection(connectionOptions)

  /**
   * The ZLayer that creates the MsSqlDataProvider.
   */
  val layer: ZLayer[ConnectionOptions, Nothing, MsSqlConnection & SchemaProvider[ArcaneSchema]] =
    ZLayer.scoped {
      ZIO.fromAutoCloseable{
        for connectionOptions <- ZIO.service[ConnectionOptions] yield MsSqlConnection(connectionOptions)
      }
    }

  /**
   * Represents a batch of data.
   */
  type DataBatch = QueryResult[LazyQueryResult.OutputType] & CanPeekHead[LazyQueryResult.OutputType]
  
  /**
   * Represents a batch of data that can be used to backfill the data.
   * Since the data is not versioned, the version is always 0,
   * and we don't need to be able to peek the head of the result.
   */
  type BackfillBatch = QueryResult[LazyQueryResult.OutputType]

  /**
   * Represents a versioned batch of data.
   */
  type VersionedBatch = (DataBatch, Long)

  /**
   * Ensures that the head of the result (if any) saved and cannot be lost
   * This is required to let the head function work properly.
   */
  private def ensureHead(result: VersionedBatch): VersionedBatch =
    val (queryResult, version) = result
    (queryResult.peekHead, version)


