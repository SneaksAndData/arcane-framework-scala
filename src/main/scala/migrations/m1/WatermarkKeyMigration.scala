package com.sneaksanddata.arcane.framework
package migrations.m1

import migrations.base.Migration
import models.MetadataKeys
import models.app.PluginStreamContext
import models.settings.TableNaming.parts
import services.iceberg.base.{SinkEntityManager, SinkPropertyManager}

import zio.ZIO

class WatermarkKeyMigration extends Migration:
  override type RequiredMigrationServices = PluginStreamContext & SinkPropertyManager

  private val legacyPropertyName = "arcane-watermark"
  val description: String =
    "Ensures watermark value is stored under `arcane.core.watermark` property, changed from `arcane-watermark`"
  val shortName: String = "watermark_key"
  val uniqueId: Long    = 61492573L

  def execute: ZIO[RequiredMigrationServices, Throwable, Unit] = for
    sinkPropertyManager <- ZIO.service[SinkPropertyManager]
    context             <- ZIO.service[PluginStreamContext]
    // find watermark value
    legacyWatermark <- sinkPropertyManager.getProperty(context.sink.targetTableFullName.parts.name, legacyPropertyName)
    _ <- ZIO.when(legacyWatermark.isDefined)(
      sinkPropertyManager.setProperty(
        context.sink.targetTableFullName.parts.name,
        MetadataKeys.getFrameworkKey(MetadataKeys.watermarkKey),
        legacyWatermark.get
      )
    )
  yield ()
