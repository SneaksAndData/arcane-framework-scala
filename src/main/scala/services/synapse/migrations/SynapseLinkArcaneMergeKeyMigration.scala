package com.sneaksanddata.arcane.framework
package services.synapse.migrations

import migrations.base.{MergeKeyColumnRebuild, Migration}
import models.app.PluginStreamContext
import models.schemas.MergeKeyField
import services.base.{MergeServiceClient, StreamingSource}
import services.iceberg.base.SinkPropertyManager

import zio.ZIO

class SynapseLinkArcaneMergeKeyMigration extends Migration:
  private val legacyIdFieldName = "Id"

  val description: String =
    s"Updates ${MergeKeyField.name} column values for Synapse Link source using unified murmur3 computation"
  val shortName: String = "arcane_merge_key"
  val uniqueId: Long    = 84729104L

  def execute(
      sinkPropertyManager: SinkPropertyManager,
      context: PluginStreamContext,
      mergeClient: MergeServiceClient,
      streamingSource: StreamingSource
  ): ZIO[Any, Throwable, Unit] =
    val targetTable  = context.sink.targetTableFullName
    val mergeKeyExpr = s"lower(to_hex(murmur3(to_utf8(cast($legacyIdFieldName as varchar)))))"

    MergeKeyColumnRebuild(targetTable, mergeKeyExpr, sinkPropertyManager, mergeClient, shortName)
