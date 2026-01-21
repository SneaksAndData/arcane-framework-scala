package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.streaming

import logging.ZIOLogAnnotations.zlog
import models.settings.TargetTableSettings
import services.iceberg.IcebergS3CatalogWriter
import services.metrics.DeclaredMetrics
import services.streaming.base.*
import services.streaming.processors.transformers.IndexedStagedBatches

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

class WatermarkProcessor(
    icebergS3CatalogWriter: IcebergS3CatalogWriter,
    targetTableSettings: TargetTableSettings,
    declaredMetrics: DeclaredMetrics
) extends StagedBatchProcessor:
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] = ZPipeline.mapZIO { batchesSet =>
    for _ <- ZIO.foreach(batchesSet.groupedBySchema) { batch =>
        ZIO.when(batch.completedWatermarkValue.isDefined) {
          for
            watermark <- ZIO.succeed(batch.completedWatermarkValue.get)
            _ <- zlog(s"Batch ${batch.name} completed stream from watermark $watermark, will updating target watermark")
            previousWatermark <- icebergS3CatalogWriter.getProperty(targetTableSettings.targetTableFullName, "comment")
            _                 <- icebergS3CatalogWriter.comment(targetTableSettings.targetTableFullName, watermark)
            _                 <- zlog(s"Updated watermark from $previousWatermark to $watermark")
          yield ()
        }
      }
    yield batchesSet
  }

object WatermarkProcessor:
  def apply(
      icebergS3CatalogWriter: IcebergS3CatalogWriter,
      targetTableSettings: TargetTableSettings,
      declaredMetrics: DeclaredMetrics
  ): WatermarkProcessor =
    new WatermarkProcessor(icebergS3CatalogWriter, targetTableSettings, declaredMetrics)

  /** The required environment for the WatermarkProcessor.
    */
  type Environment = IcebergS3CatalogWriter & TargetTableSettings & DeclaredMetrics

  /** The ZLayer that creates the WatermarkProcessor.
    */
  val layer: ZLayer[Environment, Nothing, WatermarkProcessor] =
    ZLayer {
      for
        iceberg             <- ZIO.service[IcebergS3CatalogWriter]
        targetTableSettings <- ZIO.service[TargetTableSettings]
        declaredMetrics     <- ZIO.service[DeclaredMetrics]
      yield WatermarkProcessor(iceberg, targetTableSettings, declaredMetrics)
    }
