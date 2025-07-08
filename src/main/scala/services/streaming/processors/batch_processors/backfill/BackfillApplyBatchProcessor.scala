package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.backfill

import logging.ZIOLogAnnotations.*
import models.batches.StagedBackfillOverwriteBatch
import services.base.MergeServiceClient
import services.merging.JdbcTableManager
import services.streaming.base.StreamingBatchProcessor

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/** The streaming batch processor that processes the Backfill batches produced by the backfill data provider running in
  * the backfill mode with the backfill behavior set to overwrite.
  */
class BackfillApplyBatchProcessor(mergeServiceClient: MergeServiceClient, tableManager: JdbcTableManager)
    extends StreamingBatchProcessor:

  override type BatchType = StagedBackfillOverwriteBatch

  /** Processes the incoming data.
    *
    * @return
    *   ZPipeline (stream source for the stream graph).
    */
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] =
    ZPipeline.mapZIO(batch =>
      for
        _ <- zlog("Applying backfill batch with name to %s", batch.targetTableName)
        _ <- tableManager.migrateSchema(batch.schema, batch.targetTableName)
        _ <- mergeServiceClient.applyBatch(batch)
      yield batch
    )

object BackfillApplyBatchProcessor:

  /** Factory method to create MergeProcessor
    *
    * @param mergeServiceClient
    *   The JDBC consumer.
    * @param tableManager
    *   The table manager.
    * @param targetTableSettings
    *   The target table settings.
    * @return
    *   The initialized MergeProcessor instance
    */
  def apply(mergeServiceClient: MergeServiceClient, tableManager: JdbcTableManager): BackfillApplyBatchProcessor =
    new BackfillApplyBatchProcessor(mergeServiceClient, tableManager)

  /** The required environment for the BackfillMergeBatchProcessor.
    */
  type Environment = MergeServiceClient & JdbcTableManager

  /** The ZLayer that creates the MergeProcessor.
    */
  val layer: ZLayer[Environment, Nothing, BackfillApplyBatchProcessor] =
    ZLayer {
      for
        jdbcConsumer        <- ZIO.service[MergeServiceClient]
        parallelismSettings <- ZIO.service[JdbcTableManager]
      yield BackfillApplyBatchProcessor(jdbcConsumer, parallelismSettings)
    }
