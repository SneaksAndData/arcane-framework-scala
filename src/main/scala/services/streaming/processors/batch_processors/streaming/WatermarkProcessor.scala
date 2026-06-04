package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.streaming

import logging.ZIOLogAnnotations.zlog
import models.app.PluginStreamContext
import models.settings.TableNaming.*
import models.settings.backfill.BackfillBehavior.Merge
import models.settings.sink.SinkSettings
import services.iceberg.base.SinkPropertyManager
import services.metrics.DeclaredMetrics
import services.streaming.base.*
import services.streaming.processors.batch_processors.WatermarkProcessingExtensions.*

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

class WatermarkProcessor(
    propertyManager: SinkPropertyManager,
    targetTableShortName: String,
    declaredMetrics: DeclaredMetrics,
    finishOnApply: Boolean
) extends StagedBatchProcessor:
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] = ZPipeline[BatchType].mapZIO { batch =>
    for 
      _ <- batch.applyWatermark(
        propertyManager,
        targetTableShortName,
        declaredMetrics,
        "WatermarkProcessor"
      )
      _ <- ZIO.when(finishOnApply)(zlog("Stream is configured to finish on watermark update, will terminate now"))
    yield if finishOnApply then
        None
      else
        Some(batch)  
  }.takeWhile(_.isDefined).map(_.get)

object WatermarkProcessor:
  def apply(
      propertyManager: SinkPropertyManager,
      targetTableShortName: String,
      declaredMetrics: DeclaredMetrics,
      finishOnApply: Boolean
  ): WatermarkProcessor =
    new WatermarkProcessor(propertyManager, targetTableShortName, declaredMetrics, finishOnApply)

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
      yield WatermarkProcessor(iceberg, context.sink.targetTableFullName.parts.name, declaredMetrics, context.streamMode.backfill.backfillBehavior == Merge)
    }
