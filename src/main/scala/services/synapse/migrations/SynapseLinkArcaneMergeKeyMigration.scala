package com.sneaksanddata.arcane.framework
package services.synapse.migrations

import logging.ZIOLogAnnotations.zlog
import migrations.base.Migration
import models.app.PluginStreamContext
import models.schemas.MergeKeyField
import services.base.{MergeServiceClient, StreamingSource}
import services.iceberg.base.SinkPropertyManager

import zio.ZIO

class SynapseLinkArcaneMergeKeyMigration extends Migration:
  val description: String =
    "Updates ARCANE_MERGE_KEY column values for Synapse Link source using unified murmur3 computation"
  val shortName: String = "arcane_merge_key"
  val uniqueId: Long    = 84729104L

  def execute(
      sinkPropertyManager: SinkPropertyManager,
      context: PluginStreamContext,
      mergeClient: MergeServiceClient,
      streamingSource: StreamingSource
  ): ZIO[Any, Throwable, Unit] =
    val mergeKeyExpr = "lower(to_hex(murmur3(to_utf8(cast(Id as varchar)))))"
    val updateQuery =
      s"UPDATE ${context.sink.targetTableFullName} SET ${MergeKeyField.name} = $mergeKeyExpr"

    for
      _ <- zlog("Executing ARCANE_MERGE_KEY migration: %s", updateQuery)
      _ <- mergeClient.execute(updateQuery, shortName, "Migrating")
      _ <- zlog("Successfully migrated ARCANE_MERGE_KEY for %s", context.sink.targetTableFullName)
    yield ()
