package com.sneaksanddata.arcane.framework
package services.backfill.processors

import logging.ZIOLogAnnotations.{getAnnotation, zlog}
import models.settings.TableNaming.*
import models.sharding.{CompletedShard, CompletionShard}
import services.backfill.base.StagedShardProcessor
import services.base.MergeServiceClient
import services.iceberg.base.SinkPropertyManager
import services.metrics.DeclaredMetrics
import services.streaming.base.*

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

class BackfillCompletionProcessor(
    propertyManager: SinkPropertyManager,
    mergeServiceClient: MergeServiceClient,
    declaredMetrics: DeclaredMetrics
) extends StagedShardProcessor:

  override type IncomingElement = CompletionShard
  override type OutgoingElement = CompletedShard

  override def process: ZPipeline[Any, Throwable, IncomingElement, OutgoingElement] = ZPipeline[IncomingElement]
    .mapZIO { shard =>
      for
        _                 <- zlog("All shards have been combined in %s, ready for target swap", shard.combinedTableName)
        _                 <- mergeServiceClient.commitShard(shard)
        _                 <- zlog("Target %s updated, will now update watermark", shard.targetTableName)
        previousWatermark <- propertyManager.getRequiredProperty(shard.targetTableName.parts.name, "comment")
        _                 <- propertyManager.comment(shard.targetTableName.parts.name, shard.watermark)
        _ <- zlog(
          "Updated watermark from %s to %s",
          Seq(getAnnotation("processor", "BackfillWatermarkProcessor")),
          previousWatermark,
          shard.watermark
        )
        _ <- ZIO.attempt(
          TimestampOnlyWatermark.fromJson(shard.watermark).age.toDouble
        ) @@ declaredMetrics.watermarkAge
      yield shard.toCompleted
    }

object BackfillCompletionProcessor:
  def apply(
      propertyManager: SinkPropertyManager,
      mergeServiceClient: MergeServiceClient,
      declaredMetrics: DeclaredMetrics
  ): BackfillCompletionProcessor =
    new BackfillCompletionProcessor(propertyManager, mergeServiceClient, declaredMetrics)

  /** The required environment for the BackfillWatermarkProcessor.
    */
  type Environment = SinkPropertyManager & MergeServiceClient & DeclaredMetrics

  /** The ZLayer that creates the BackfillWatermarkProcessor.
    */
  val layer: ZLayer[Environment, Nothing, BackfillCompletionProcessor] =
    ZLayer {
      for
        iceberg            <- ZIO.service[SinkPropertyManager]
        mergeServiceClient <- ZIO.service[MergeServiceClient]
        declaredMetrics    <- ZIO.service[DeclaredMetrics]
      yield BackfillCompletionProcessor(
        iceberg,
        mergeServiceClient,
        declaredMetrics
      )
    }
