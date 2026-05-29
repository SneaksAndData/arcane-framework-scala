package com.sneaksanddata.arcane.framework
package services.mssql

import models.schemas.MergeKeyField
import models.settings.mssql.MsSqlServerDatabaseSourceSettings
import services.mssql.base.{ColumnSummary, MsSqlQuery, MsSqlStreamingSource}

import zio.{Task, ZIO}

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import scala.io.Source
import scala.math.{log, pow}

object QueryProvider:
  /** The key used to merge rows in the output table.
    */
  private val UPSERT_MERGE_KEY = MergeKeyField.name

  /** Gets the schema query for the Microsoft SQL Server database.
    *
    * msSqlConnection The connection to the database.
    * @return
    *   A future containing the schema query for the Microsoft SQL Server database.
    */
  extension (reader: MsSqlStreamingSource)
    def getSchemaQuery: Task[MsSqlQuery] =
      for
        columnSummaries <- reader.getColumnSummaries(
          reader.connectionSettings.schemaName,
          reader.connectionSettings.tableName
        )
        mergeExpression  = QueryProvider.getMergeExpression(columnSummaries, "tq")
        columnExpression = QueryProvider.getChangeTrackingColumns(columnSummaries, "ct", "tq")
        matchStatement   = QueryProvider.getMatchStatement(columnSummaries, "ct", "tq", None)
        query <- QueryProvider.getChangesQuery(
          reader.connectionSettings,
          reader.catalog,
          mergeExpression,
          columnExpression,
          matchStatement,
          Long.MaxValue
        )
      yield query

  /** Gets the changes query for the Microsoft SQL Server database.
    *
    * @param msSqlConnection
    *   The connection to the database.
    * @param fromVersion
    *   The version to start from.
    * @return
    *   A future containing the changes query for the Microsoft SQL Server database.
    */
  extension (reader: MsSqlStreamingSource)
    def getChangesQuery(fromVersion: Long): Task[MsSqlQuery] =
      for
        columnSummaries <- reader.getColumnSummaries(
          reader.connectionSettings.schemaName,
          reader.connectionSettings.tableName
        )
        mergeExpression  = QueryProvider.getMergeExpression(columnSummaries, "ct")
        columnExpression = QueryProvider.getChangeTrackingColumns(columnSummaries, "ct", "tq")
        matchStatement   = QueryProvider.getMatchStatement(columnSummaries, "ct", "tq", None)
        query <- QueryProvider.getChangesQuery(
          reader.connectionSettings,
          reader.catalog,
          mergeExpression,
          columnExpression,
          matchStatement,
          fromVersion
        )
      yield query

  /** Gets the changes query for the Microsoft SQL Server database.
    *
    * @param msSqlConnection
    *   The connection to the database.
    * @return
    *   A future containing the changes query for the Microsoft SQL Server database.
    */
  extension (reader: MsSqlStreamingSource)
    def getBackfillQuery(shardSchemaName: String, shardTableName: String): Task[MsSqlQuery] =
      for
        columnSummaries <- reader.getColumnSummaries(reader.connectionSettings.schemaName, shardTableName)
        mergeExpression  = QueryProvider.getMergeExpression(columnSummaries, "tq")
        columnExpression = QueryProvider.getChangeTrackingColumns(columnSummaries, "tq")
        query <- QueryProvider.getAllQuery(
          reader.connectionSettings,
          reader.catalog,
          shardSchemaName,
          shardTableName,
          mergeExpression,
          columnExpression
        )
      yield query

  /** Gets the column summaries query for the Microsoft SQL Server database.
    *
    * @param schemaName
    *   The name of the schema.
    * @param tableName
    *   The name of the table.
    * @param databaseName
    *   The name of the database.
    * @return
    *   The column summaries query for the Microsoft SQL Server database.
    */
  def getColumnSummariesQuery(schemaName: String, tableName: String, databaseName: String): Task[MsSqlQuery] =
    ZIO.scoped {
      for
        source <- ZIO.fromAutoCloseable(ZIO.attempt(Source.fromResource("get_column_summaries.sql")))
        query = source
          .getLines()
          .mkString("\n")
          .replace("{dbName}", databaseName)
          .replace("{schema}", schemaName)
          .replace("{table}", tableName)
      yield query
    }

  def getFindMatchingTablesQuery(tablePrefix: String, schemaName: String): MsSqlQuery =
    s"""SELECT 
       |  t.name
       |FROM 
       |    sys.tables t
       |INNER JOIN 
       |    sys.schemas s ON t.schema_id = s.schema_id and s.name = '$schemaName'
       |WHERE 
       |    t.name LIKE '$tablePrefix%'""".stripMargin

  def getCreatePrimaryKeyQuery(
      shardSchemaName: String,
      shardTableName: String,
      summary: List[ColumnSummary]
  ): MsSqlQuery =
    val pkString = summary.filter(_._2).map(v => s"[${v._1}]").mkString(",")
    s"""ALTER TABLE [$shardSchemaName].[$shardTableName] ADD PRIMARY KEY CLUSTERED ($pkString)"""

  def getFillShardQuery(
      sourceSchemaName: String,
      sourceTableName: String,
      shardSchemaName: String,
      shardTableName: String,
      mergeExpression: String,
      shardCount: Int,
      shardId: Int
  ): MsSqlQuery =
    s"""INSERT INTO [$shardSchemaName].[$shardTableName]
      |SELECT *
      |FROM [$sourceSchemaName].[$sourceTableName] as tq
      |WHERE ABS(CAST(HASHBYTES('MD5', $mergeExpression) AS BIGINT)) % $shardCount = $shardId""".stripMargin

  def getCreateCloneQuery(
      sourceSchemaName: String,
      sourceTableName: String,
      targetSchemaName: String,
      targetTableName: String
  ): MsSqlQuery =
    s"""SELECT *
      |INTO [$targetSchemaName].[$targetTableName]
      |FROM [$sourceSchemaName].[$sourceTableName]
      |WHERE 1 = 0""".stripMargin

  def getStatsProfileQuery(schemaName: String, tableName: String): MsSqlQuery =
    s"""EXEC('
      | SET STATISTICS PROFILE ON;
      | SELECT TOP 1 * FROM [$schemaName].[$tableName];
      | SET STATISTICS PROFILE OFF')""".stripMargin

  private def costToSize(cost: Double): Double =
    val calculatedCost = 1.0 + pow(log(cost), 3)
    // Hard cap at 1000
    if (calculatedCost > 1000.0) 1000.0 else calculatedCost

  def getSourcePhysicalStatsQuery(schemaName: String, tableName: String, cost: Double): MsSqlQuery =
    val shardSizeEstimate = costToSize(cost)
    s"""SELECT
     |    (page_count * 8.0) / 1024 / 1024 as total_size_gib,
     |    ceiling((page_count * 8.0) / 1024 / $shardSizeEstimate) as shards,
     |    record_count / ceiling((page_count * 8.0) / 1024 / $shardSizeEstimate) as records_per_shard
     |FROM
     |    sys.dm_db_index_physical_stats(DB_ID(), OBJECT_ID('$schemaName.$tableName'), 1, NULL, 'DETAILED')
     |where index_level = 0""".stripMargin

  def getSourcePhysicalStatsQuery(schemaName: String, tableName: String, shardSize: Int): MsSqlQuery =
    s"""SELECT
       |    (page_count * 8.0) / 1024 / 1024 as total_size_gib,
       |    ceiling((page_count * 8.0) / 1024 / $shardSize) as shards,
       |    record_count / cast((page_count * 8.0) / 1024 / ceiling((page_count * 8.0) / 1024 / $shardSize) as records_per_shard
       |FROM
       |    sys.dm_db_index_physical_stats(DB_ID(), OBJECT_ID('$schemaName.$tableName'), 1, NULL, 'DETAILED')
       |where index_level = 0""".stripMargin

  /** Gets the query that retrieves the change tracking version for the Microsoft SQL Server database, based on the
    * provided startFrom timestamp point. The look back range for the query.
    * @return
    *   The change tracking version query for the Microsoft SQL Server database.
    */
  def getVersionFromTimestampQuery(startFrom: OffsetDateTime, formatter: DateTimeFormatter): MsSqlQuery =
    val formattedTime = formatter.format(startFrom)
    s"SELECT MIN(commit_ts) FROM sys.dm_tran_commit_table WHERE commit_time >= '$formattedTime'"

  /** Retrieve commit time associated with the provided version
    */
  def getVersionCommitTime(version: Long): MsSqlQuery =
    s"SELECT MIN(commit_time) FROM sys.dm_tran_commit_table WHERE commit_ts = $version"

  /** Return latest change tracking version at the time of a call
    * @return
    */
  def getCurrentVersionQuery: MsSqlQuery =
    s"SELECT CHANGE_TRACKING_CURRENT_VERSION()"

  def getMergeExpression(cs: List[ColumnSummary], tableAlias: String): String =
    cs.filter((name, isPrimaryKey) => isPrimaryKey)
      .map((name, _) => s"cast($tableAlias.[$name] as nvarchar(128))")
      .mkString(" + '#' + ")

  private def getMatchStatement(
      cs: List[ColumnSummary],
      sourceAlias: String,
      outputAlias: String,
      partitionColumns: Option[List[String]]
  ): String =
    val mainMatch = cs
      .filter((_, isPrimaryKey) => isPrimaryKey)
      .map((name, _) => s"$outputAlias.[$name] = $sourceAlias.[$name]")
      .mkString(" and ")

    partitionColumns match
      case Some(columns) =>
        val partitionMatch = columns
          .map(column => s"$outputAlias.[$column] = $sourceAlias.[$column]")
          .mkString(" and ")
        s"$mainMatch and  ($sourceAlias.SYS_CHANGE_OPERATION == 'D' OR ($partitionMatch))"
      case None => mainMatch

  private def getChangeTrackingColumns(
      tableColumns: List[ColumnSummary],
      changesAlias: String,
      tableAlias: String
  ): String =
    val primaryKeyColumns =
      tableColumns.filter((_, isPrimaryKey) => isPrimaryKey).map((name, _) => s"$changesAlias.[$name]")
    val additionalColumns = List(s"$changesAlias.SYS_CHANGE_VERSION", s"$changesAlias.SYS_CHANGE_OPERATION")
    val nonPrimaryKeyColumns = tableColumns
      .filter((name, isPrimaryKey) =>
        !isPrimaryKey && !Set("SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION").contains(name)
      )
      .map((name, _) => s"$tableAlias.[$name]")
    (primaryKeyColumns ++ additionalColumns ++ nonPrimaryKeyColumns).mkString(",\n")

  private def getChangeTrackingColumns(tableColumns: List[ColumnSummary], tableAlias: String): String =
    val primaryKeyColumns =
      tableColumns.filter((_, isPrimaryKey) => isPrimaryKey).map((name, _) => s"$tableAlias.[$name]")
    val additionalColumns = List("CAST(0 as BIGINT) as SYS_CHANGE_VERSION", "'I' as SYS_CHANGE_OPERATION")
    val nonPrimaryKeyColumns = tableColumns
      .filter((name, isPrimaryKey) =>
        !isPrimaryKey && !Set("SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION").contains(name)
      )
      .map((name, _) => s"$tableAlias.[$name]")

    (primaryKeyColumns ++ additionalColumns ++ nonPrimaryKeyColumns).mkString(",\n")

  private def getChangesQuery(
      connectionSettings: MsSqlServerDatabaseSourceSettings,
      databaseName: String,
      mergeExpression: String,
      columnStatement: String,
      matchStatement: String,
      changeTrackingId: Long
  ): Task[MsSqlQuery] =
    ZIO.scoped {
      for
        querySource <- ZIO.fromAutoCloseable {
          ZIO.attempt(Source.fromResource("get_select_delta_query.sql"))
        }
        baseQuery <- ZIO.attempt(querySource.getLines().mkString("\n"))
        query = baseQuery
          .replace("{dbName}", databaseName)
          .replace("{schema}", connectionSettings.schemaName)
          .replace("{tableName}", connectionSettings.tableName)
          .replace("{ChangeTrackingColumnsStatement}", columnStatement)
          .replace("{ChangeTrackingMatchStatement}", matchStatement)
          .replace("{MERGE_EXPRESSION}", mergeExpression)
          .replace("{MERGE_KEY}", MergeKeyField.name)
          .replace("{lastId}", changeTrackingId.toString)
      yield query
    }

  private def getAllQuery(
      connectionSettings: MsSqlServerDatabaseSourceSettings,
      databaseName: String,
      schemaName: String,
      tableName: String,
      mergeExpression: String,
      columnExpression: String
  ): Task[MsSqlQuery] =
    ZIO.scoped {
      for
        querySource <- ZIO.fromAutoCloseable {
          ZIO.attempt(Source.fromResource("get_select_all_query.sql"))
        }
        baseQuery <- ZIO.attempt(querySource.getLines().mkString("\n"))
        query = baseQuery
          .replace("{dbName}", databaseName)
          .replace("{schema}", schemaName)
          .replace("{tableName}", tableName)
          .replace("{ChangeTrackingColumnsStatement}", columnExpression)
          .replace("{MERGE_EXPRESSION}", mergeExpression)
          .replace("{MERGE_KEY}", MergeKeyField.name)
      yield query
    }
