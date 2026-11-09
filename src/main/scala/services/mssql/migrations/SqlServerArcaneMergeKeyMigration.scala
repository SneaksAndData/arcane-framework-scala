package com.sneaksanddata.arcane.framework
package services.mssql.migrations

import migrations.base.{MergeKeyColumnRebuild, Migration}
import models.app.PluginStreamContext
import models.schemas.MergeKeyField
import services.base.{MergeServiceClient, StreamingSource}
import services.iceberg.base.SinkPropertyManager
import services.mssql.SqlDataCell.normalizeName
import services.mssql.base.ColumnSummary

import zio.ZIO

class SqlServerArcaneMergeKeyMigration(columnSummaries: List[ColumnSummary]) extends Migration:
  val description: String =
    s"Updates ${MergeKeyField.name} column values for SQL Server source using unified murmur3 computation"
  val shortName: String = "arcane_merge_key"
  val uniqueId: Long    = 84729104L

  def execute(
      sinkPropertyManager: SinkPropertyManager,
      context: PluginStreamContext,
      mergeClient: MergeServiceClient,
      streamingSource: StreamingSource
  ): ZIO[Any, Throwable, Unit] =
    val primaryKeys      = columnSummaries.collect { case (name, true) => name.normalizeName }
    val concatExpression = primaryKeys.map(pk => s"cast($pk as varchar)").mkString(" || '#' || ")
    val mergeKeyExpr     = s"lower(to_hex(murmur3(to_utf8($concatExpression))))"

    MergeKeyColumnRebuild(
      context.sink.targetTableFullName,
      mergeKeyExpr,
      sinkPropertyManager,
      mergeClient,
      shortName
    )
