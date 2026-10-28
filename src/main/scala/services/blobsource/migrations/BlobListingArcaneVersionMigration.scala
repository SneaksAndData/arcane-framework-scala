package com.sneaksanddata.arcane.framework
package services.blobsource.migrations

import logging.ZIOLogAnnotations.zlog
import migrations.base.Migration
import models.app.PluginStreamContext
import models.batches.BlobBatchCommons
import models.schemas.VersionField
import models.settings.TableNaming.parts
import services.base.{MergeServiceClient, StreamingSource}
import services.iceberg.base.SinkPropertyManager

import zio.ZIO

import scala.jdk.CollectionConverters.*

/** Prior to `ARCANE_VERSION` being populated as a surrogate version column (see `BlobListingStreamingSource`),
  * `createdon` was carried through to the sink table, and `ARCANE_VERSION` did not exist on the sink table at all. This
  * migration adds `ARCANE_VERSION` when it is missing, copies the existing `createdon` value into it, and then drops
  * the legacy `createdon` column, now that it is only used internally to derive `ARCANE_VERSION` and is no longer
  * emitted by the source.
  */
class BlobListingArcaneVersionMigration extends Migration:
  private val legacyVersionFieldName = BlobBatchCommons.versionField.name

  val description: String =
    s"Copies $legacyVersionFieldName into ARCANE_VERSION, then drops the legacy $legacyVersionFieldName column from the sink " +
      "table, now that it is only used internally to derive ARCANE_VERSION"
  val shortName: String = "arcane_version"
  val uniqueId: Long    = 73960215L

  def execute(
      sinkPropertyManager: SinkPropertyManager,
      context: PluginStreamContext,
      mergeClient: MergeServiceClient,
      streamingSource: StreamingSource
  ): ZIO[Any, Throwable, Unit] =
    val targetTable = context.sink.targetTableFullName
    for
      existingColumns <- sinkPropertyManager
        .getTableSchema(targetTable.parts.name)
        .map(_.columns().asScala.map(_.name()).toSet)
      _ <- ZIO.when(existingColumns.exists(_.equalsIgnoreCase(legacyVersionFieldName))) {
        val hasVersionColumn = existingColumns.exists(_.equalsIgnoreCase(VersionField.name))
        val updateQuery      = s"UPDATE $targetTable SET ${VersionField.name} = cast($legacyVersionFieldName as bigint)"
        val dropColumnQuery  = s"ALTER TABLE $targetTable DROP COLUMN $legacyVersionFieldName"
        for
          _ <- ZIO.unless(hasVersionColumn) {
            val addColumnQuery = s"ALTER TABLE $targetTable ADD COLUMN ${VersionField.name} bigint"
            for
              _ <- zlog("Adding missing ARCANE_VERSION column: %s", addColumnQuery)
              _ <- mergeClient.execute(addColumnQuery, shortName, "Migrating")
              _ <- zlog("Successfully added ARCANE_VERSION column to %s", targetTable)
            yield ()
          }
          _ <- zlog("Executing ARCANE_VERSION migration: %s", updateQuery)
          _ <- mergeClient.execute(updateQuery, shortName, "Migrating")
          _ <- zlog("Successfully migrated ARCANE_VERSION for %s", targetTable)
          _ <- zlog("Dropping legacy column: %s", dropColumnQuery)
          _ <- mergeClient.execute(dropColumnQuery, shortName, "Migrating")
          _ <- zlog("Successfully dropped legacy column %s from %s", legacyVersionFieldName, targetTable)
        yield ()
      }
    yield ()
