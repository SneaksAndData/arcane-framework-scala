package com.sneaksanddata.arcane.framework
package services.mssql

import models.MergeKeyField
import models.DatePartitionField

import org.slf4j.{Logger, LoggerFactory}

import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}
import scala.concurrent.Future
import scala.io.Source
import scala.util.{Try, Using}

object QueryProvider:
  /**
   * The key used to merge rows in the output table.
   */
  private val UPSERT_MERGE_KEY = MergeKeyField.name

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  /**
   * Gets the schema query for the Microsoft SQL Server database.
   *
   * @param msSqlConnection The connection to the database.
   * @return A future containing the schema query for the Microsoft SQL Server database.
   */
  extension (msSqlConnection: MsSqlConnection) def getSchemaQuery: Future[MsSqlQuery] =
    msSqlConnection.getColumnSummaries
      .flatMap(columnSummaries => {
        val mergeExpression = QueryProvider.getMergeExpression(columnSummaries, "tq")
        val columnExpression = QueryProvider.getChangeTrackingColumns(columnSummaries, "ct", "tq")
        val matchStatement = QueryProvider.getMatchStatement(columnSummaries, "ct", "tq", None)
        Future.fromTry(QueryProvider.getChangesQuery(
          msSqlConnection.connectionOptions,
          mergeExpression,
          columnExpression,
          matchStatement,
          Long.MaxValue))
      })

  /**
   * Gets the changes query for the Microsoft SQL Server database.
   *
   * @param msSqlConnection The connection to the database.
   * @param fromVersion     The version to start from.
   * @return A future containing the changes query for the Microsoft SQL Server database.
   */
  extension (msSqlConnection: MsSqlConnection) def getChangesQuery(fromVersion: Long): Future[MsSqlQuery] =
    msSqlConnection.getColumnSummaries
      .flatMap(columnSummaries => {
        val mergeExpression = QueryProvider.getMergeExpression(columnSummaries, "tq")
        val columnExpression = QueryProvider.getChangeTrackingColumns(columnSummaries, "ct", "tq")
        val matchStatement = QueryProvider.getMatchStatement(columnSummaries, "ct", "tq", None)
        Future.fromTry(QueryProvider.getChangesQuery(
          msSqlConnection.connectionOptions,
          mergeExpression,
          columnExpression,
          matchStatement,
          fromVersion))
      })

  /**
   * Gets the changes query for the Microsoft SQL Server database.
   *
   * @param msSqlConnection The connection to the database.
   * @return A future containing the changes query for the Microsoft SQL Server database.
   */
  extension (msSqlConnection: MsSqlConnection) def getBackfillQuery: Future[MsSqlQuery] =
    msSqlConnection.getColumnSummaries
      .flatMap(columnSummaries => {
        val mergeExpression = QueryProvider.getMergeExpression(columnSummaries, "tq")
        val columnExpression = QueryProvider.getChangeTrackingColumns(columnSummaries, "tq")
        Future.fromTry(QueryProvider.getAllQuery(
          msSqlConnection.connectionOptions,
          mergeExpression,
          columnExpression))
      })

  /**
   * Gets the column summaries query for the Microsoft SQL Server database.
   *
   * @param schemaName   The name of the schema.
   * @param tableName    The name of the table.
   * @param databaseName The name of the database.
   * @return The column summaries query for the Microsoft SQL Server database.
   */
  def getColumnSummariesQuery(schemaName: String, tableName: String, databaseName: String): Try[MsSqlQuery] =
    Using(Source.fromResource("get_column_summaries.sql")) { source =>
      source
        .getLines
        .mkString("\n")
        .replace("{dbName}", databaseName)
        .replace("{schema}", schemaName)
        .replace("{table}", tableName)
    }

  /**
   * Gets the query that retrieves the change tracking version for the Microsoft SQL Server database.
   *
   * @param databaseName  The name of the database.
   * @param maybeVersion  The version to start from.
   * @param lookBackRange The look back range for the query.
   * @return The change tracking version query for the Microsoft SQL Server database.
   */
  def getChangeTrackingVersionQuery(databaseName: String, maybeVersion: Option[Long], lookBackRange: Duration)
                                   (using formatter: DateTimeFormatter): MsSqlQuery = {
    maybeVersion match
      case None =>
        val lookBackTime = Instant.now().minusSeconds(lookBackRange.getSeconds)
        val formattedTime = formatter.format(LocalDateTime.ofInstant(lookBackTime, ZoneOffset.UTC))
        s"SELECT MIN(commit_ts) FROM $databaseName.sys.dm_tran_commit_table WHERE commit_time > '$formattedTime'"
      case Some(version) => s"SELECT MIN(commit_ts) FROM $databaseName.sys.dm_tran_commit_table WHERE commit_ts > $version"
  }

  private def getMergeExpression(cs: List[ColumnSummary], tableAlias: String): String =
    cs.filter((name, isPrimaryKey) => isPrimaryKey)
      .map((name, _) => s"cast($tableAlias.[$name] as nvarchar(128))")
      .mkString(" + '#' + ")

  private def getMatchStatement(cs: List[ColumnSummary], sourceAlias: String, outputAlias: String, partitionColumns: Option[List[String]]): String =
    val mainMatch = cs.filter((_, isPrimaryKey) => isPrimaryKey)
      .map((name, _) => s"$outputAlias.[$name] = $sourceAlias.[$name]")
      .mkString(" and ")

    partitionColumns match
      case Some(columns) =>
        val partitionMatch = columns
          .map(column => s"$outputAlias.[$column] = $sourceAlias.[$column]")
          .mkString(" and ")
        s"$mainMatch and  ($sourceAlias.SYS_CHANGE_OPERATION == 'D' OR ($partitionMatch))"
      case None => mainMatch


  private def getChangeTrackingColumns(tableColumns: List[ColumnSummary], changesAlias: String, tableAlias: String): String =
    val primaryKeyColumns = tableColumns.filter((_, isPrimaryKey) => isPrimaryKey).map((name, _) => s"$changesAlias.[$name]")
    val additionalColumns = List(s"$changesAlias.SYS_CHANGE_VERSION", s"$changesAlias.SYS_CHANGE_OPERATION")
    val nonPrimaryKeyColumns = tableColumns
      .filter((name, isPrimaryKey) => !isPrimaryKey && !Set("SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION").contains(name))
      .map((name, _) => s"$tableAlias.[$name]")
    (primaryKeyColumns ++ additionalColumns ++ nonPrimaryKeyColumns).mkString(",\n")

  private def getChangeTrackingColumns(tableColumns: List[ColumnSummary], tableAlias: String): String =
    val primaryKeyColumns = tableColumns.filter((_, isPrimaryKey) => isPrimaryKey).map((name, _) => s"$tableAlias.[$name]")
    val additionalColumns = List("CAST(0 as BIGINT) as SYS_CHANGE_VERSION", "'I' as SYS_CHANGE_OPERATION")
    val nonPrimaryKeyColumns = tableColumns
      .filter((name, isPrimaryKey) => !isPrimaryKey && !Set("SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION").contains(name))
      .map((name, _) => s"$tableAlias.[$name]")

    (primaryKeyColumns ++ additionalColumns ++ nonPrimaryKeyColumns).mkString(",\n")

  private def getChangesQuery(connectionOptions: ConnectionOptions,
                              mergeExpression: String,
                              columnStatement: String,
                              matchStatement: String,
                              changeTrackingId: Long): Try[MsSqlQuery] =
    val querySource = connectionOptions.partitionExpression match {
      case Some(_) => Source.fromResource("get_select_delta_query_date_partitioned.sql")
      case None => Source.fromResource("get_select_delta_query.sql")
    }

    val query = Using(querySource)(_.getLines.mkString("\n")) map { baseQuery =>
      baseQuery.replace("{dbName}", connectionOptions.databaseName)
        .replace("{schema}", connectionOptions.schemaName)
        .replace("{tableName}", connectionOptions.tableName)
        .replace("{ChangeTrackingColumnsStatement}", columnStatement)
        .replace("{ChangeTrackingMatchStatement}", matchStatement)
        .replace("{MERGE_EXPRESSION}", mergeExpression)
        .replace("{MERGE_KEY}", MergeKeyField.name)
        .replace("{DATE_PARTITION_EXPRESSION}", connectionOptions.partitionExpression.getOrElse(""))
        .replace("{DATE_PARTITION_KEY}", DatePartitionField.name)
        .replace("{lastId}", changeTrackingId.toString)
    }
    logger.debug(s"Query: $query")
    query

  private def getAllQuery(connectionOptions: ConnectionOptions,
                          mergeExpression: String,
                          columnExpression: String): Try[MsSqlQuery] =

    val querySource = connectionOptions.partitionExpression match {
      case Some(_) => Source.fromResource("get_select_all_query_date_partitioned.sql")
      case None => Source.fromResource("get_select_all_query.sql")
    }

    val query = Using(querySource)(_.getLines.mkString("\n")) map { baseQuery =>
      baseQuery
        .replace("{dbName}", connectionOptions.databaseName)
        .replace("{schema}", connectionOptions.schemaName)
        .replace("{tableName}", connectionOptions.tableName)
        .replace("{ChangeTrackingColumnsStatement}", columnExpression)
        .replace("{MERGE_EXPRESSION}", mergeExpression)
        .replace("{MERGE_KEY}", MergeKeyField.name)
        .replace("{DATE_PARTITION_EXPRESSION}", connectionOptions.partitionExpression.getOrElse(""))
        .replace("{DATE_PARTITION_KEY}", DatePartitionField.name)
    }
    logger.debug(s"Query: $query")
    query
