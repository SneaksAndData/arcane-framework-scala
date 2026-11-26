package com.sneaksanddata.arcane.framework
package services.blobsource.migrations

import exceptions.FatalStreamFailException
import logging.ZIOLogAnnotations.zlog
import migrations.base.{Migration, MigrationQueryHelper}
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
    s"Copies $legacyVersionFieldName into ${VersionField.name}, then drops the legacy $legacyVersionFieldName column from the sink " +
      s"table, now that it is only used internally to derive ${VersionField.name}"
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
      hasVersionColumn = existingColumns.exists(_.equalsIgnoreCase(VersionField.name))
      hasNulls <- MigrationQueryHelper
        .hasNulls(targetTable, VersionField.name, context)
        .when(hasVersionColumn)
        .someOrElse(true)
      hasLegacyColumn = existingColumns.exists(_.equalsIgnoreCase(legacyVersionFieldName))

      _ <- (hasVersionColumn, hasNulls, hasLegacyColumn) match
        case (true, false, true) =>
          val dropColumnQuery = s"ALTER TABLE $targetTable DROP COLUMN $legacyVersionFieldName"
          zlog("Dropping legacy column: %s", dropColumnQuery) *>
            mergeClient.execute(dropColumnQuery, shortName, "Migrating") *>
            zlog("Successfully dropped legacy column %s from %s", legacyVersionFieldName, targetTable)

        case (true, false, false) =>
          zlog("%s is already migrated on %s", VersionField.name, targetTable)

        case (_, _, false) =>
          ZIO.fail(
            FatalStreamFailException(
              s"Table '$targetTable' is not eligible for migration: '${VersionField.name}' is missing or contains NULL values, and legacy version column '$legacyVersionFieldName' does not exist to migrate from."
            )
          )

        case (hasVersion, _, true) =>
          val updateQuery = s"UPDATE $targetTable SET ${VersionField.name} = cast($legacyVersionFieldName as bigint)"
          val dropColumnQuery = s"ALTER TABLE $targetTable DROP COLUMN $legacyVersionFieldName"
          for
            _ <- ZIO.unless(hasVersion) {
              val addColumnQuery = s"ALTER TABLE $targetTable ADD COLUMN ${VersionField.name} bigint"
              zlog("Adding missing %s column: %s", VersionField.name, addColumnQuery) *>
                mergeClient.execute(addColumnQuery, shortName, "Migrating") *>
                zlog("Successfully added %s column to %s", VersionField.name, targetTable)
            }
            _ <- zlog("Executing %s migration: %s", VersionField.name, updateQuery)
            _ <- mergeClient.execute(updateQuery, shortName, "Migrating")
            _ <- zlog("Successfully migrated %s for %s", VersionField.name, targetTable)
            _ <- zlog("Dropping legacy column: %s", dropColumnQuery)
            _ <- mergeClient.execute(dropColumnQuery, shortName, "Migrating")
            _ <- zlog("Successfully dropped legacy column %s from %s", legacyVersionFieldName, targetTable)
          yield ()
    yield ()
