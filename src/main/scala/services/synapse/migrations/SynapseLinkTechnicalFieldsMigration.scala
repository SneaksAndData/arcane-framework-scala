package com.sneaksanddata.arcane.framework
package services.synapse.migrations

import logging.ZIOLogAnnotations.zlog
import migrations.base.Migration
import models.app.PluginStreamContext
import models.schemas.VersionField
import models.settings.TableNaming.parts
import services.base.{MergeServiceClient, StreamingSource}
import services.iceberg.base.SinkPropertyManager

import zio.ZIO

import scala.jdk.CollectionConverters.*

/** Prior to `Id` and `versionnumber` being treated purely as technical fields used to derive `ARCANE_MERGE_KEY` and
  * `ARCANE_VERSION` (see `SynapseLinkStreamingSource`), both columns were carried through to the sink table as regular
  * output columns, and `ARCANE_VERSION` did not exist on the sink table at all. This migration adds `ARCANE_VERSION`
  * when it is missing, copies the existing `versionnumber` value into it, and then drops the legacy `Id` and
  * `versionnumber` columns, now that they are only used internally to derive `ARCANE_MERGE_KEY` and `ARCANE_VERSION`
  * and are no longer emitted by the source.
  *
  * This migration must run after [[SynapseLinkArcaneMergeKeyMigration]], which still relies on `Id` being present on
  * the sink table to (re)compute `ARCANE_MERGE_KEY`.
  */
class SynapseLinkTechnicalFieldsMigration extends Migration:
  private val legacyVersionFieldName = "versionnumber"
  private val legacyColumnNames      = Seq("Id", legacyVersionFieldName)

  val description: String =
    "Copies versionnumber into ARCANE_VERSION, then drops the legacy Id and versionnumber columns from the sink " +
      "table, now that they are only used internally to derive ARCANE_MERGE_KEY and ARCANE_VERSION"
  val shortName: String = "arcane_technical_fields"
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
        for
          _ <- ZIO.unless(hasVersionColumn) {
            val addColumnQuery = s"ALTER TABLE $targetTable ADD COLUMN ${VersionField.name} bigint"
            for
              _ <- zlog("Adding missing ARCANE_VERSION column: %s", addColumnQuery)
              _ <- mergeClient.execute(addColumnQuery, shortName, "Migrating")
              _ <- zlog("Successfully added ARCANE_VERSION column to %s", targetTable)
            yield ()
          }
          updateQuery = s"UPDATE $targetTable SET ${VersionField.name} = cast($legacyVersionFieldName as bigint)"
          _ <- zlog("Executing ARCANE_VERSION migration: %s", updateQuery)
          _ <- mergeClient.execute(updateQuery, shortName, "Migrating")
          _ <- zlog("Successfully migrated ARCANE_VERSION for %s", targetTable)
        yield ()
      }
      columnsToDrop = legacyColumnNames.filter(name => existingColumns.exists(_.equalsIgnoreCase(name)))
      _ <- ZIO.foreachDiscard(columnsToDrop) { columnName =>
        val dropColumnQuery = s"ALTER TABLE $targetTable DROP COLUMN $columnName"
        for
          _ <- zlog("Dropping legacy column: %s", dropColumnQuery)
          _ <- mergeClient.execute(dropColumnQuery, shortName, "Migrating")
          _ <- zlog("Successfully dropped legacy column %s from %s", columnName, targetTable)
        yield ()
      }
    yield ()
