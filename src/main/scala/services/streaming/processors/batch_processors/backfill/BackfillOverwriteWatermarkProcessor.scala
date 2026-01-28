package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.backfill

import models.batches.StagedBackfillOverwriteBatch
import models.settings.SinkSettings
import services.iceberg.base.TablePropertyManager
import services.metrics.DeclaredMetrics
import services.streaming.base.*
import services.streaming.processors.batch_processors.WatermarkProcessingExtensions.*

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

class BackfillOverwriteWatermarkProcessor(
    propertyManager: TablePropertyManager,
    targetTableSettings: SinkSettings,
    declaredMetrics: DeclaredMetrics
) extends StreamingBatchProcessor:

  override type BatchType = StagedBackfillOverwriteBatch

  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] = ZPipeline.mapZIO { batch =>
    for _ <- batch.applyWatermark(
        propertyManager,
        targetTableSettings.targetTableNameParts.Name,
        declaredMetrics
      )
    yield batch
  }

object BackfillOverwriteWatermarkProcessor:
  def apply(
      propertyManager: TablePropertyManager,
      targetTableSettings: SinkSettings,
      declaredMetrics: DeclaredMetrics
  ): BackfillOverwriteWatermarkProcessor =
    new BackfillOverwriteWatermarkProcessor(propertyManager, targetTableSettings, declaredMetrics)

  /** The required environment for the BackfillOverwriteWatermarkProcessor.
    */
  type Environment = TablePropertyManager & SinkSettings & DeclaredMetrics

  /** The ZLayer that creates the BackfillOverwriteWatermarkProcessor.
    */
  val layer: ZLayer[Environment, Nothing, BackfillOverwriteWatermarkProcessor] =
    ZLayer {
      for
        iceberg             <- ZIO.service[TablePropertyManager]
        targetTableSettings <- ZIO.service[SinkSettings]
        declaredMetrics     <- ZIO.service[DeclaredMetrics]
      yield BackfillOverwriteWatermarkProcessor(iceberg, targetTableSettings, declaredMetrics)
    }
