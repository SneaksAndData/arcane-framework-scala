package com.sneaksanddata.arcane.framework
package services.backfill.processors

import logging.ZIOLogAnnotations.{getAnnotation, zlog}
import models.app.PluginStreamContext
import models.batches.{StagedBackfillOverwriteBatch, StagedBatch}
import models.settings.TableNaming.*
import models.settings.sink.SinkSettings
import models.sharding.{CompletedShard, CompletionShard}
import services.backfill.StagedShardProcessor
import services.iceberg.base.SinkPropertyManager
import services.metrics.DeclaredMetrics
import services.streaming.base.*
import services.streaming.processors.batch_processors.WatermarkProcessingExtensions.*

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

class BackfillWatermarkProcessor(
    propertyManager: SinkPropertyManager,
    targetTableShortName: String,
    declaredMetrics: DeclaredMetrics
) extends StagedShardProcessor:

  override type IncomingElement = CompletionShard
  override type OutgoingElement = CompletedShard

  override def process: ZPipeline[Any, Throwable, IncomingElement, OutgoingElement] = ZPipeline[IncomingElement]
    .mapZIO {
    shard =>
      for
        previousWatermark <- propertyManager.getProperty(shard.targetTableName, "comment")
        _                 <- propertyManager.comment(shard.targetTableName, shard.watermark.toJson)
        _ <- zlog(
          "Updated watermark from %s to %s",
          Seq(getAnnotation("processor", "BackfillWatermarkProcessor")),
          previousWatermark,
          shard.watermark.toJson
        )
        _ <- ZIO.attempt(
          TimestampOnlyWatermark.fromJson(shard.watermark.toJson).age.toDouble
        ) @@ declaredMetrics.appliedWatermarkAge
      yield shard.toCompleted
  }

object BackfillWatermarkProcessor:
  def apply(
      propertyManager: SinkPropertyManager,
      targetTableShortName: String,
      declaredMetrics: DeclaredMetrics
  ): BackfillWatermarkProcessor =
    new BackfillWatermarkProcessor(propertyManager, targetTableShortName, declaredMetrics)

  /** The required environment for the BackfillWatermarkProcessor.
    */
  type Environment = SinkPropertyManager & PluginStreamContext & DeclaredMetrics

  /** The ZLayer that creates the BackfillWatermarkProcessor.
    */
  val layer: ZLayer[Environment, Nothing, BackfillWatermarkProcessor] =
    ZLayer {
      for
        iceberg         <- ZIO.service[SinkPropertyManager]
        context         <- ZIO.service[PluginStreamContext]
        declaredMetrics <- ZIO.service[DeclaredMetrics]
      yield BackfillWatermarkProcessor(iceberg, context.sink.targetTableFullName.parts.name, declaredMetrics)
    }
