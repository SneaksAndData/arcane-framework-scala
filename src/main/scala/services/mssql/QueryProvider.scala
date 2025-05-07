package com.sneaksanddata.arcane.framework
package services.mssql

import models.schemas.MergeKeyField

import zio.{Task, ZIO}

import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}
import scala.io.Source

object QueryProvider:
  /**
   * The key used to merge rows in the output table.
   */
  private val UPSERT_MERGE_KEY = MergeKeyField.name

  /**
   * Gets the schema query for the Microsoft SQL Server database.
   *
   * msSqlConnection The connection to the database.
   * @return A future containing the schema query for the Microsoft SQL Server database.
   */
  extension (msSqlConnection: MsSqlConnection) def getSchemaQuery: Task[MsSqlQuery] =
    for columnSummaries <- msSqlConnection.getColumnSummaries
        mergeExpression = QueryProvider.getMergeExpression(columnSummaries, "tq")
        columnExpression = QueryProvider.getChangeTrackingColumns(columnSummaries, "ct", "tq")
        matchStatement = QueryProvider.getMatchStatement(columnSummaries, "ct", "tq", None)
        query <- QueryProvider.getChangesQuery(
          msSqlConnection.connectionOptions,
          msSqlConnection.catalog,
          mergeExpression,
          columnExpression,
          matchStatement,
          Long.MaxValue)
    yield query

  /**
   * Gets the changes query for the Microsoft SQL Server database.
   *
   * @param msSqlConnection The connection to the database.
   * @param fromVersion     The version to start from.
   * @return A future containing the changes query for the Microsoft SQL Server database.
   */
  extension (msSqlConnection: MsSqlConnection) def getChangesQuery(fromVersion: Long): Task[MsSqlQuery] =
    for columnSummaries <- msSqlConnection.getColumnSummaries
        mergeExpression = QueryProvider.getMergeExpression(columnSummaries, "ct")
        columnExpression = QueryProvider.getChangeTrackingColumns(columnSummaries, "ct", "tq")
        matchStatement = QueryProvider.getMatchStatement(columnSummaries, "ct", "tq", None)
        query <- QueryProvider.getChangesQuery(
          msSqlConnection.connectionOptions,
          msSqlConnection.catalog,
          mergeExpression,
          columnExpression,
          matchStatement,
          fromVersion)
    yield query

  /**
   * Gets the changes query for the Microsoft SQL Server database.
   *
   * @param msSqlConnection The connection to the database.
   * @return A future containing the changes query for the Microsoft SQL Server database.
   */
  extension (msSqlConnection: MsSqlConnection) def getBackfillQuery: Task[MsSqlQuery] =
    for columnSummaries <- msSqlConnection.getColumnSummaries
        mergeExpression = QueryProvider.getMergeExpression(columnSummaries, "tq")
        columnExpression = QueryProvider.getChangeTrackingColumns(columnSummaries, "tq")
        query <- QueryProvider.getAllQuery(msSqlConnection.connectionOptions, msSqlConnection.catalog, mergeExpression, columnExpression)
    yield query

  /**
   * Gets the column summaries query for the Microsoft SQL Server database.
   *
   * @param schemaName   The name of the schema.
   * @param tableName    The name of the table.
   * @param databaseName The name of the database.
   * @return The column summaries query for the Microsoft SQL Server database.
   */
  def getColumnSummariesQuery(schemaName: String, tableName: String, databaseName: String): Task[MsSqlQuery] =
    ZIO.scoped {
      for source <- ZIO.fromAutoCloseable(ZIO.attempt(Source.fromResource("get_column_summaries.sql")))
          query = source
            .getLines
            .mkString("\n")
            .replace("{dbName}", databaseName)
            .replace("{schema}", schemaName)
            .replace("{table}", tableName)
      yield query
    }

  /**
   * Gets the query that retrieves the change tracking version for the Microsoft SQL Server database.
   *
   * @param maybeVersion  The version to start from.
   * @param lookBackRange The look back range for the query.
   * @return The change tracking version query for the Microsoft SQL Server database.
   */
  def getChangeTrackingVersionQuery(maybeVersion: Option[Long], lookBackRange: Duration)(using formatter: DateTimeFormatter): MsSqlQuery = {
    maybeVersion match
      case None =>
        val lookBackTime = Instant.now().minusSeconds(lookBackRange.getSeconds)
        val formattedTime = formatter.format(LocalDateTime.ofInstant(lookBackTime, ZoneOffset.UTC))
        s"SELECT MIN(commit_ts) FROM sys.dm_tran_commit_table WHERE commit_time > '$formattedTime'"
      case Some(version) => s"SELECT MIN(commit_ts) FROM sys.dm_tran_commit_table WHERE commit_ts > $version"
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
                              databaseName: String,
                              mergeExpression: String,
                              columnStatement: String,
                              matchStatement: String,
                              changeTrackingId: Long): Task[MsSqlQuery] =
    ZIO.scoped {
      for querySource <- ZIO.fromAutoCloseable {
            ZIO.attempt(Source.fromResource("get_select_delta_query.sql"))
          }
          baseQuery <- ZIO.attempt(querySource.getLines.mkString("\n"))
          query = baseQuery.replace("{dbName}", databaseName)
            .replace("{schema}", connectionOptions.schemaName)
            .replace("{tableName}", connectionOptions.tableName)
            .replace("{ChangeTrackingColumnsStatement}", columnStatement)
            .replace("{ChangeTrackingMatchStatement}", matchStatement)
            .replace("{MERGE_EXPRESSION}", mergeExpression)
            .replace("{MERGE_KEY}", MergeKeyField.name)
            .replace("{lastId}", changeTrackingId.toString)
      yield query
    }

  private def getAllQuery(connectionOptions: ConnectionOptions,
                          databaseName: String,
                          mergeExpression: String,
                          columnExpression: String): Task[MsSqlQuery] =
    ZIO.scoped {
      for querySource <- ZIO.fromAutoCloseable {
            ZIO.attempt(Source.fromResource("get_select_all_query.sql"))
          }
          baseQuery <- ZIO.attempt(querySource.getLines.mkString("\n"))
          query = baseQuery
            .replace("{dbName}", databaseName)
            .replace("{schema}", connectionOptions.schemaName)
            .replace("{tableName}", connectionOptions.tableName)
            .replace("{ChangeTrackingColumnsStatement}", columnExpression)
            .replace("{MERGE_EXPRESSION}", mergeExpression)
            .replace("{MERGE_KEY}", MergeKeyField.name)
      yield query
    }
