package com.sneaksanddata.arcane.framework
package services.pullstream.migrations

import logging.ZIOLogAnnotations.zlog
import migrations.base.Migration
import models.app.PluginStreamContext
import models.schemas.VersionField
import services.base.{MergeServiceClient, StreamingSource}
import services.iceberg.base.SinkPropertyManager

import zio.ZIO

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
    "Moves the pull stream watermark column into ARCANE_VERSION (as epoch millis) and drops the legacy column"
  val shortName: String = "arcane_version_field"
  val uniqueId: Long    = 95713208L

  def execute(
      sinkPropertyManager: SinkPropertyManager,
      context: PluginStreamContext,
      mergeClient: MergeServiceClient,
      streamingSource: StreamingSource
  ): ZIO[Any, Throwable, Unit] =
    val targetTable = context.sink.targetTableFullName
    val versionExpr =
      s"cast(to_unixtime(from_iso8601_timestamp($legacyVersionFieldName)) * 1000 as bigint)"
    val updateQuery =
      s"UPDATE $targetTable SET ${VersionField.name} = $versionExpr"
    val dropColumnQuery =
      s"ALTER TABLE $targetTable DROP COLUMN $legacyVersionFieldName"

    for
      _ <- zlog("Executing ARCANE_VERSION migration: %s", updateQuery)
      _ <- mergeClient.execute(updateQuery, shortName, "Migrating")
      _ <- zlog("Successfully migrated ARCANE_VERSION for %s", targetTable)
      _ <- zlog("Dropping legacy watermark column: %s", dropColumnQuery)
      _ <- mergeClient.execute(dropColumnQuery, shortName, "Migrating")
      _ <- zlog("Successfully dropped legacy watermark column %s from %s", legacyVersionFieldName, targetTable)
    yield ()
