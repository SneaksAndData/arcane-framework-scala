package com.sneaksanddata.arcane.framework
package services.synapse.migrations

import logging.ZIOLogAnnotations.zlog
import migrations.base.{MergeKeyColumnRebuild, Migration}
import models.app.PluginStreamContext
import models.schemas.MergeKeyField
import models.settings.TableNaming.parts
import services.base.{MergeServiceClient, StreamingSource}
import services.iceberg.base.SinkPropertyManager

import zio.ZIO

import scala.jdk.CollectionConverters.*

class SynapseLinkArcaneMergeKeyMigration extends Migration:
  private val legacyIdFieldName = "Id"

  val description: String =
    s"Updates ${MergeKeyField.name} column values for Synapse Link source using unified murmur3 computation, " +
      "then drops the legacy Id column from the sink table"
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
    val dropQuery    = s"ALTER TABLE $targetTable DROP COLUMN $legacyIdFieldName"

    for
      existingColumns <- sinkPropertyManager
        .getTableSchema(targetTable.parts.name)
        .map(_.columns().asScala.map(_.name()).toSet)
      _ <- ZIO.when(existingColumns.exists(_.equalsIgnoreCase(legacyIdFieldName))) {
        for
          _ <- MergeKeyColumnRebuild(targetTable, mergeKeyExpr, sinkPropertyManager, mergeClient, shortName)
          _ <- zlog("Dropping legacy column: %s", dropQuery)
          _ <- mergeClient.execute(dropQuery, shortName, "Migrating")
          _ <- zlog("Successfully dropped legacy column %s from %s", legacyIdFieldName, targetTable)
        yield ()
      }
    yield ()
