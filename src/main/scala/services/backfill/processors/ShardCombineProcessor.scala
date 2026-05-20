package com.sneaksanddata.arcane.framework
package services.backfill.processors

import logging.ZIOLogAnnotations.*
import models.backfill.DefaultSourceBackfill
import models.backfill.DefaultSourceBackfill.toJson
import models.sharding.{CompletionShard, StagedShard}
import services.backfill.StagedShardProcessor
import services.backfill.processors.ShardStagingProcessor
import services.base.MergeServiceClient
import services.iceberg.base.{SinkEntityManager, SinkPropertyManager, StagingPropertyManager}
import services.streaming.base.{JsonWatermark, StreamingBatchProcessor}

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/** Combines incoming shards and outputs the completion shard containing the watermark
  */
class ShardCombineProcessor(
    mergeServiceClient: MergeServiceClient,
    watermark: JsonWatermark
) extends StagedShardProcessor:

  override type IncomingElement = ShardStagingProcessor#OutgoingElement
  override type OutgoingElement = CompletionShard

  /** Processes the incoming data.
    *
    * @return
    *   ZPipeline (stream source for the stream graph).
    */
  override def process: ZPipeline[Any, Throwable, IncomingElement, OutgoingElement] =
    ZPipeline[StagedShard]
      .mapZIO { staged =>
        for
          _ <- zlog("Shard %s fully commited into %s, ready for combine", staged.shardId, staged.shardTableName)
          _ <- mergeServiceClient.commitShard(staged)
          shard <- ZIO.succeed(
            CompletionShard(
              watermark.toJson,
              staged.targetTableName,
              staged.shardSourceEntityName,
              staged.shardSourceEntityName,
              staged.combinedTableName
            )
          )
        yield shard
      }

object ShardCombineProcessor:

  /** Factory method to create MergeProcessor
    *
    * @param mergeServiceClient
    *   The JDBC consumer.
    * @return
    *   The initialized MergeProcessor instance
    */
  def apply(
      mergeServiceClient: MergeServiceClient,
      watermark: JsonWatermark
  ): ShardCombineProcessor =
    new ShardCombineProcessor(mergeServiceClient, watermark)
