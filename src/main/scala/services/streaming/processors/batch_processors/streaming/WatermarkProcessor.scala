package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.streaming

import models.app.PluginStreamContext
import models.settings.TableNaming.*
import services.iceberg.base.SinkPropertyManager
import services.metrics.DeclaredMetrics
import services.streaming.base.*
import services.streaming.processors.batch_processors.WatermarkProcessingExtensions.*

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

class WatermarkProcessor(
    propertyManager: SinkPropertyManager,
    targetTableShortName: String,
    declaredMetrics: DeclaredMetrics
) extends StagedBatchProcessor:
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] = ZPipeline[BatchType]
    .mapZIO { batch =>
      for _ <- batch.applyWatermark(
          propertyManager,
          targetTableShortName,
          declaredMetrics,
          "WatermarkProcessor"
        )
      yield batch
    }

object WatermarkProcessor:
  def apply(
      propertyManager: SinkPropertyManager,
      targetTableShortName: String,
      declaredMetrics: DeclaredMetrics
  ): WatermarkProcessor =
    new WatermarkProcessor(propertyManager, targetTableShortName, declaredMetrics)

  /** The required environment for the WatermarkProcessor.
    */
  type Environment = SinkPropertyManager & PluginStreamContext & DeclaredMetrics

  /** The ZLayer that creates the WatermarkProcessor.
    */
  val layer: ZLayer[Environment, Nothing, WatermarkProcessor] =
    ZLayer {
      for
        iceberg         <- ZIO.service[SinkPropertyManager]
        context         <- ZIO.service[PluginStreamContext]
        declaredMetrics <- ZIO.service[DeclaredMetrics]
      yield WatermarkProcessor(
        iceberg,
        context.sink.targetTableFullName.parts.name,
        declaredMetrics
      )
    }
