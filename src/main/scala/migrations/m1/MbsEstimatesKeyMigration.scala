package com.sneaksanddata.arcane.framework
package migrations.m1

import logging.ZIOLogAnnotations.zlog
import migrations.base.Migration
import models.app.PluginStreamContext
import models.settings.TableNaming.parts
import services.base.{MergeServiceClient, StreamingSource}
import services.iceberg.base.SinkPropertyManager
import services.streaming.throughput.MemoryBoundShaper

import zio.ZIO

class MbsEstimatesKeyMigration extends Migration:
  private val legacyPropertyName = "mbs-estimates"
  val description: String =
    "Ensures throughput estimates value is stored under `arcane.core.throughput.mbs.estimates` property, changed from `mbs-estimates`"
  val shortName: String = "mbs_estimates_key"
  val uniqueId: Long    = 48291047L

  def execute(
      sinkPropertyManager: SinkPropertyManager,
      context: PluginStreamContext,
      mergeClient: MergeServiceClient,
      streamingSource: StreamingSource
  ): ZIO[Any, Throwable, Unit] =
    val targetTableName = context.sink.targetTableFullName.parts.name
    val newKey          = MemoryBoundShaper.estimatePropertyKey

    for
      legacyValue   <- sinkPropertyManager.getProperty(targetTableName, legacyPropertyName)
      existingValue <- sinkPropertyManager.getProperty(targetTableName, newKey)
      _ <- ZIO.when(existingValue.isEmpty && legacyValue.isDefined) {
        for
          _ <- zlog("Found throughput estimates in legacy key `%s`: %s", legacyPropertyName, legacyValue.get)
          _ <- sinkPropertyManager.setProperty(targetTableName, newKey, legacyValue.get)
          _ <- zlog("Set throughput estimates property %s=%s on %s", newKey, legacyValue.get, targetTableName)
        yield ()
      }
      _ <- ZIO.when(legacyValue.isDefined) {
        for
          _ <- sinkPropertyManager.removeProperty(targetTableName, legacyPropertyName)
          _ <- zlog("Removed legacy property %s from %s", legacyPropertyName, targetTableName)
        yield ()
      }
      _ <- ZIO.unless(legacyValue.isDefined)(zlog("No legacy throughput estimates on table %s", targetTableName))
    yield ()
