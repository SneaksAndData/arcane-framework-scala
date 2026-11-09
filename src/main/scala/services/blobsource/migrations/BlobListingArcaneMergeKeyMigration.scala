package com.sneaksanddata.arcane.framework
package services.blobsource.migrations

import migrations.base.{MergeKeyColumnRebuild, Migration}
import models.app.PluginStreamContext
import models.schemas.MergeKeyField
import services.base.{MergeServiceClient, StreamingSource}
import services.iceberg.base.SinkPropertyManager

import zio.ZIO

class BlobListingArcaneMergeKeyMigration(primaryKeys: Seq[String]) extends Migration:
  val description: String =
    s"Updates ${MergeKeyField.name} column values for blob listing source using unified murmur3 computation"
  val shortName: String = "arcane_merge_key"
  val uniqueId: Long    = 84729104L

  def execute(
      sinkPropertyManager: SinkPropertyManager,
      context: PluginStreamContext,
      mergeClient: MergeServiceClient,
      streamingSource: StreamingSource
  ): ZIO[Any, Throwable, Unit] =
    val concatExpression = primaryKeys.map(pk => s"cast($pk as varchar)").mkString(" || '#' || ")
    val mergeKeyExpr     = s"lower(to_hex(murmur3(to_utf8($concatExpression))))"

    MergeKeyColumnRebuild(
      context.sink.targetTableFullName,
      mergeKeyExpr,
      sinkPropertyManager,
      mergeClient,
      shortName
    )
