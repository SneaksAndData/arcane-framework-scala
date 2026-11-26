package com.sneaksanddata.arcane.framework
package migrations.m1

import exceptions.FatalStreamFailException
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
    s"Ensures watermark value is stored under `${MetadataKeys.getFrameworkKey(MetadataKeys.watermarkKey)}` property, changed from `$legacyPropertyName`"
  val shortName: String = "watermark_key"
  val uniqueId: Long    = 61492573L

  def execute(
      sinkPropertyManager: SinkPropertyManager,
      context: PluginStreamContext,
      mergeClient: MergeServiceClient,
      streamingSource: StreamingSource
  ): ZIO[Any, Throwable, Unit] =
    val targetTableName = context.sink.targetTableFullName.parts.name
    val newKey          = MetadataKeys.getFrameworkKey(MetadataKeys.watermarkKey)

    for
      currentWatermark <- sinkPropertyManager.getProperty(targetTableName, newKey)
      legacyWatermark  <- sinkPropertyManager.getProperty(targetTableName, legacyPropertyName)
      _ <- (currentWatermark.filter(_.nonEmpty), legacyWatermark.filter(_.nonEmpty)) match
        case (Some(current), Some(_)) =>
          zlog("Target table %s already has watermark under key %s=%s", targetTableName, newKey, current) *>
            sinkPropertyManager.removeProperty(targetTableName, legacyPropertyName)

        case (Some(current), None) =>
          zlog("Target table %s already has watermark under key %s=%s", targetTableName, newKey, current)

        case (None, Some(legacy)) =>
          for
            _ <- zlog("Found watermark value in legacy key `%s`: %s", legacyPropertyName, legacy)
            _ <- sinkPropertyManager.setProperty(targetTableName, newKey, legacy)
            _ <- sinkPropertyManager.removeProperty(targetTableName, legacyPropertyName)
            _ <- zlog("Set watermark value using key %s=%s", newKey, legacy)
          yield ()

        case (None, None) =>
          ZIO.fail(
            FatalStreamFailException(
              s"Table '$targetTableName' is not eligible for migration: neither '$newKey' nor legacy '$legacyPropertyName' property is present."
            )
          )
    yield ()
