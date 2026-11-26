package com.sneaksanddata.arcane.framework
package services.pullstream.migrations

import exceptions.FatalStreamFailException
import logging.ZIOLogAnnotations.zlog
import migrations.base.{Migration, MigrationQueryHelper}
import models.app.PluginStreamContext
import models.schemas.VersionField
import models.settings.TableNaming.parts
import services.base.{MergeServiceClient, StreamingSource}
import services.iceberg.base.SinkPropertyManager

import zio.ZIO

import scala.jdk.CollectionConverters.*

/** Prior to the `dattr__`-prefixed envelope merge (see `PullStreamingSource.responseStream`), the DynamoDB watermark
  * attribute was written straight into the sink's literal `versionFieldName` column, and that column was used to order
  * concurrent versions during a merge. It is now read out of the envelope into `ARCANE_VERSION` instead, so a table
  * provisioned under the old behavior needs its existing watermark values carried across before the legacy column is
  * dropped: without this, `ARCANE_VERSION` would start out NULL and a table merged before and after the change
  * respectively would briefly compare an unset ordering column against a populated one.
  *
  * @param legacyVersionFieldName
  *   Name of the sink column the DynamoDB watermark attribute used to be written to verbatim.
  */
class PullStreamVersionFieldMigration(legacyVersionFieldName: String) extends Migration:
  val description: String =
    s"Moves the pull stream watermark column into ${VersionField.name} (as epoch millis) and drops the legacy column"
  val shortName: String = "arcane_version_field"
  val uniqueId: Long    = 95713208L

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
          zlog("Dropping legacy watermark column: %s", dropColumnQuery) *>
            mergeClient.execute(dropColumnQuery, shortName, "Migrating") *>
            zlog("Successfully dropped legacy watermark column %s from %s", legacyVersionFieldName, targetTable)

        case (true, false, false) =>
          zlog("%s is already migrated on %s", VersionField.name, targetTable)

        case (_, _, false) =>
          ZIO.fail(
            FatalStreamFailException(
              s"Table '$targetTable' is not eligible for migration: '${VersionField.name}' is missing or contains NULL values, and legacy version column '$legacyVersionFieldName' does not exist to migrate from."
            )
          )

        case (hasVersion, _, true) =>
          val versionExpr =
            s"cast(to_unixtime(from_iso8601_timestamp($legacyVersionFieldName)) * 1000 as bigint)"
          val updateQuery     = s"UPDATE $targetTable SET ${VersionField.name} = $versionExpr"
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
            _ <- zlog("Dropping legacy watermark column: %s", dropColumnQuery)
            _ <- mergeClient.execute(dropColumnQuery, shortName, "Migrating")
            _ <- zlog("Successfully dropped legacy watermark column %s from %s", legacyVersionFieldName, targetTable)
          yield ()
    yield ()
