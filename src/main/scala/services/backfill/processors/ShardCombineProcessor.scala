package com.sneaksanddata.arcane.framework
package services.backfill.processors

import logging.ZIOLogAnnotations.*
import models.sharding.{CompletionShard, StagedShard}
import services.backfill.base.{ShardFactory, StagedShardProcessor}
import services.backfill.processors.ShardStagingProcessor
import services.base.MergeServiceClient
import services.streaming.base.JsonWatermark

import zio.ZIO
import zio.stream.ZPipeline

/** Combines incoming shards and outputs the completion shard containing the watermark
  */
class ShardCombineProcessor(
    mergeServiceClient: MergeServiceClient,
    shardFactory: ShardFactory,
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
        yield shardFactory.createCompletionShard(staged, watermark.toJson)
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
      shardFactory: ShardFactory,
      watermark: JsonWatermark
  ): ShardCombineProcessor =
    new ShardCombineProcessor(mergeServiceClient, shardFactory, watermark)
