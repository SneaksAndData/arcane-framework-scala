package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.streaming

import models.settings.SinkSettings
import services.iceberg.base.TablePropertyManager
import services.metrics.DeclaredMetrics
import services.streaming.base.*
import services.streaming.processors.batch_processors.WatermarkProcessingExtensions.*

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

class WatermarkProcessor(
    propertyManager: TablePropertyManager,
    targetTableSettings: SinkSettings,
    declaredMetrics: DeclaredMetrics
) extends StagedBatchProcessor:
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] = ZPipeline.mapZIO { batchesSet =>
    for _ <- batchesSet.groupedBySchema.applyWatermark(
          propertyManager,
          targetTableSettings.targetTableNameParts.Name,
          declaredMetrics,
          "WatermarkProcessor"
        )
    yield batchesSet
  }

object WatermarkProcessor:
  def apply(
      propertyManager: TablePropertyManager,
      targetTableSettings: SinkSettings,
      declaredMetrics: DeclaredMetrics
  ): WatermarkProcessor =
    new WatermarkProcessor(propertyManager, targetTableSettings, declaredMetrics)

  /** The required environment for the WatermarkProcessor.
    */
  type Environment = TablePropertyManager & SinkSettings & DeclaredMetrics

  /** The ZLayer that creates the WatermarkProcessor.
    */
  val layer: ZLayer[Environment, Nothing, WatermarkProcessor] =
    ZLayer {
      for
        iceberg             <- ZIO.service[TablePropertyManager]
        targetTableSettings <- ZIO.service[SinkSettings]
        declaredMetrics     <- ZIO.service[DeclaredMetrics]
      yield WatermarkProcessor(iceberg, targetTableSettings, declaredMetrics)
    }
