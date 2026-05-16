package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.backfill

import models.app.PluginStreamContext
import models.batches.{StagedBackfillOverwriteBatch, StagedBatch}
import models.settings.sink.SinkSettings
import models.settings.TableNaming.*
import services.iceberg.base.SinkPropertyManager
import services.metrics.DeclaredMetrics
import services.streaming.base.*
import services.streaming.processors.batch_processors.WatermarkProcessingExtensions.*

import com.sneaksanddata.arcane.framework.services.backfill.processors.WatermarkShardBatch
import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

class BackfillOverwriteWatermarkProcessor(
    propertyManager: SinkPropertyManager,
    targetTableShortName: String,
    declaredMetrics: DeclaredMetrics
) extends StreamingBatchProcessor:

  override type BatchType = StagedBatch

  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] = ZPipeline.mapZIO {
    case batch: WatermarkShardBatch =>
      for _ <- batch.applyWatermark(
          propertyManager,
          targetTableShortName,
          declaredMetrics,
          "BackfillOverwriteWatermarkProcessor"
        )
    yield batch
  }

object BackfillOverwriteWatermarkProcessor:
  def apply(
      propertyManager: SinkPropertyManager,
      targetTableShortName: String,
      declaredMetrics: DeclaredMetrics
  ): BackfillOverwriteWatermarkProcessor =
    new BackfillOverwriteWatermarkProcessor(propertyManager, targetTableShortName, declaredMetrics)

  /** The required environment for the BackfillOverwriteWatermarkProcessor.
    */
  type Environment = SinkPropertyManager & PluginStreamContext & DeclaredMetrics

  /** The ZLayer that creates the BackfillOverwriteWatermarkProcessor.
    */
  val layer: ZLayer[Environment, Nothing, BackfillOverwriteWatermarkProcessor] =
    ZLayer {
      for
        iceberg         <- ZIO.service[SinkPropertyManager]
        context         <- ZIO.service[PluginStreamContext]
        declaredMetrics <- ZIO.service[DeclaredMetrics]
      yield BackfillOverwriteWatermarkProcessor(iceberg, context.sink.targetTableFullName.parts.name, declaredMetrics)
    }
