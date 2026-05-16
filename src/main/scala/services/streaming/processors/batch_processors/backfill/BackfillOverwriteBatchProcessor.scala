package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.backfill

import logging.ZIOLogAnnotations.*
import models.batches.StagedBackfillOverwriteBatch
import services.base.MergeServiceClient
import services.iceberg.base.{SinkEntityManager, SinkPropertyManager}
import services.streaming.base.StreamingBatchProcessor

import com.sneaksanddata.arcane.framework.services.backfill.processors.{ShardProcessor, StagedShardBatch, WatermarkShardBatch}
import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/** The streaming batch processor that processes the Backfill batches produced by the backfill data provider running in
  * the backfill mode with the backfill behavior set to overwrite.
  */
class BackfillOverwriteBatchProcessor(
    mergeServiceClient: MergeServiceClient,
    entityManager: SinkEntityManager,
    propertyManager: SinkPropertyManager
) extends StreamingBatchProcessor:

  override type BatchType = ShardProcessor#OutgoingElement

  /** Processes the incoming data.
    *
    * @return
    *   ZPipeline (stream source for the stream graph).
    */
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] =
  // TODO: first combine all batches in a single staging table by executing their queries
  // TODO: then run overwrite batch apply
  
    ZPipeline
      .mapZIO {
        case staged: StagedShardBatch => for
          _ <- zlog("Completed shard from the staging table %s ready for combine", staged.name)
          _ <- mergeServiceClient.applyBatch(staged)
        yield None
        case watermark: WatermarkShardBatch => for
          _ <- zlog("Watermark %s ready to be applied", watermark.completedWatermarkValue.get)
        yield Some(watermark)
      }
      .collect {
        case Some(watermark) => watermark
      }
      .mapZIO { wm => for
        _ <- zlog("All shards have been combined, swapping into target", watermark.name)
       yield wm 
      }

object BackfillOverwriteBatchProcessor:

  /** Factory method to create MergeProcessor
    *
    * @param mergeServiceClient
    *   The JDBC consumer.
    * @return
    *   The initialized MergeProcessor instance
    */
  def apply(
      mergeServiceClient: MergeServiceClient,
      entityManager: SinkEntityManager,
      propertyManager: SinkPropertyManager
  ): BackfillOverwriteBatchProcessor =
    new BackfillOverwriteBatchProcessor(mergeServiceClient, entityManager, propertyManager)

  /** The required environment for the BackfillMergeBatchProcessor.
    */
  type Environment = MergeServiceClient & SinkEntityManager & SinkPropertyManager

  /** The ZLayer that creates the MergeProcessor.
    */
  val layer: ZLayer[Environment, Nothing, BackfillOverwriteBatchProcessor] =
    ZLayer {
      for
        jdbcConsumer    <- ZIO.service[MergeServiceClient]
        entityManager   <- ZIO.service[SinkEntityManager]
        propertyManager <- ZIO.service[SinkPropertyManager]
      yield BackfillOverwriteBatchProcessor(jdbcConsumer, entityManager, propertyManager)
    }
