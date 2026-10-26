package com.sneaksanddata.arcane.framework
package migrations.m1

import logging.ZIOLogAnnotations.zlog
import migrations.base.Migration
import models.MetadataKeys
import models.app.PluginStreamContext
import models.settings.TableNaming.parts
import services.base.{MergeServiceClient, StreamingSource}
import services.iceberg.base.SinkPropertyManager

import zio.ZIO

class WatermarkKeyMigration extends Migration:
  private val legacyPropertyName = "arcane-watermark"
  val description: String =
    "Ensures watermark value is stored under `arcane.core.watermark` property, changed from `arcane-watermark`"
  val shortName: String = "watermark_key"
  val uniqueId: Long    = 61492573L

  def execute(
      sinkPropertyManager: SinkPropertyManager,
      context: PluginStreamContext,
      mergeClient: MergeServiceClient,
      streamingSource: StreamingSource
  ): ZIO[Any, Throwable, Unit] = for
    // find watermark value
    legacyWatermark <- sinkPropertyManager.getProperty(context.sink.targetTableFullName.parts.name, legacyPropertyName)
    _ <- ZIO.when(legacyWatermark.isDefined)(
      for
        _ <- zlog("Found watermark value in legacy key `arcane-watermark`: %s", legacyWatermark.get)
        newKey = MetadataKeys.getFrameworkKey(MetadataKeys.watermarkKey)
        _ <- sinkPropertyManager.setProperty(
          context.sink.targetTableFullName.parts.name,
          newKey,
          legacyWatermark.get
        )
        _ <- sinkPropertyManager.removeProperty(
          context.sink.targetTableFullName.parts.name,
          legacyPropertyName
        )
        _ <- zlog("Set watermark value using key %s=%s", newKey, legacyWatermark.get)
      yield ()
    )
    _ <- ZIO.unless(legacyWatermark.isDefined)(zlog("No legacy watermark on a table"))
  yield ()
