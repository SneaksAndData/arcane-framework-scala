package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.backfill

import logging.ZIOLogAnnotations.zlog
import models.settings.TargetTableSettings
import services.iceberg.IcebergS3CatalogWriter
import services.metrics.DeclaredMetrics
import services.streaming.base.*
import services.streaming.processors.transformers.IndexedStagedBatches

import com.sneaksanddata.arcane.framework.models.batches.StagedBackfillOverwriteBatch
import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

class BackfillOverwriteWatermarkProcessor(
    icebergS3CatalogWriter: IcebergS3CatalogWriter,
    targetTableSettings: TargetTableSettings,
    declaredMetrics: DeclaredMetrics
) extends StreamingBatchProcessor:

  override type BatchType = StagedBackfillOverwriteBatch
  
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] = ZPipeline.mapZIO { batch =>
    for
      _ <- ZIO.when(batch.completedWatermarkValue.isDefined) {
        for
          watermark <- ZIO.succeed(batch.completedWatermarkValue.get)
          _ <- zlog(s"Batch ${batch.name} completed stream from watermark $watermark, will updating target watermark")
          previousWatermark <- icebergS3CatalogWriter.getProperty(targetTableSettings.targetTableFullName, "comment")
          _ <- icebergS3CatalogWriter.comment(targetTableSettings.targetTableFullName, watermark)
          _ <- zlog(s"Updated watermark from $previousWatermark to $watermark")
        yield ()
      }
    yield batch
  }

object BackfillOverwriteWatermarkProcessor:
  def apply(
      icebergS3CatalogWriter: IcebergS3CatalogWriter,
      targetTableSettings: TargetTableSettings,
      declaredMetrics: DeclaredMetrics
  ): BackfillOverwriteWatermarkProcessor =
    new BackfillOverwriteWatermarkProcessor(icebergS3CatalogWriter, targetTableSettings, declaredMetrics)

  /** The required environment for the BackfillOverwriteWatermarkProcessor.
    */
  type Environment = IcebergS3CatalogWriter & TargetTableSettings & DeclaredMetrics

  /** The ZLayer that creates the BackfillOverwriteWatermarkProcessor.
    */
  val layer: ZLayer[Environment, Nothing, BackfillOverwriteWatermarkProcessor] =
    ZLayer {
      for
        iceberg             <- ZIO.service[IcebergS3CatalogWriter]
        targetTableSettings <- ZIO.service[TargetTableSettings]
        declaredMetrics     <- ZIO.service[DeclaredMetrics]
      yield BackfillOverwriteWatermarkProcessor(iceberg, targetTableSettings, declaredMetrics)
    }
