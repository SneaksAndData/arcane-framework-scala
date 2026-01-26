package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.streaming

import models.settings.TargetTableSettings
import services.iceberg.IcebergS3CatalogWriter
import services.metrics.DeclaredMetrics
import services.streaming.base.*
import services.streaming.processors.batch_processors.WatermarkProcessingExtensions.*

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

class WatermarkProcessor(
    icebergS3CatalogWriter: IcebergS3CatalogWriter,
    targetTableSettings: TargetTableSettings,
    declaredMetrics: DeclaredMetrics
) extends StagedBatchProcessor:
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] = ZPipeline.mapZIO { batchesSet =>
    for _ <- ZIO.foreach(batchesSet.groupedBySchema) { batch =>
        batch.applyWatermark(icebergS3CatalogWriter, targetTableSettings.targetTableNameParts.Name, declaredMetrics)
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
