package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.backfill

import logging.ZIOLogAnnotations.*
import models.batches.StagedBackfillOverwriteBatch
import services.base.MergeServiceClient
import services.iceberg.base.{SinkEntityManager, SinkPropertyManager}
import services.streaming.base.StreamingBatchProcessor

import com.sneaksanddata.arcane.framework.services.backfill.processors.ShardProcessor
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
  
    ZPipeline.mapZIO(batch =>
      for
        _ <- zlog("Applying backfill batch with name to %s", batch.name)
        _ <- mergeServiceClient.applyBatch(batch)
      yield batch
    )

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
