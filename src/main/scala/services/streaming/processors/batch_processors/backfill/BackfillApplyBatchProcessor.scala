package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.backfill

import logging.ZIOLogAnnotations.*
import models.batches.StagedBackfillOverwriteBatch
import services.base.MergeServiceClient
import services.iceberg.base.{SinkEntityManager, SinkPropertyManager}
import services.iceberg.given_Conversion_Schema_ArcaneSchema
import services.merging.JdbcTableManager
import services.streaming.base.StreamingBatchProcessor

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/** The streaming batch processor that processes the Backfill batches produced by the backfill data provider running in
  * the backfill mode with the backfill behavior set to overwrite.
  */
class BackfillApplyBatchProcessor(
    mergeServiceClient: MergeServiceClient,
    entityManager: SinkEntityManager,
    propertyManager: SinkPropertyManager
) extends StreamingBatchProcessor:

  override type BatchType = StagedBackfillOverwriteBatch

  /** Processes the incoming data.
    *
    * @return
    *   ZPipeline (stream source for the stream graph).
    */
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] =
    ZPipeline.mapZIO(batch =>
      for
        _            <- zlog("Applying backfill batch with name to %s", batch.targetTableName)
        targetSchema <- propertyManager.getTableSchema(batch.targetTableName.split(".").last)
        _            <- entityManager.migrateSchema(targetSchema, batch.schema, batch.targetTableName.split(".").last)
        _            <- mergeServiceClient.applyBatch(batch)
      yield batch
    )

object BackfillApplyBatchProcessor:

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
  ): BackfillApplyBatchProcessor =
    new BackfillApplyBatchProcessor(mergeServiceClient, entityManager, propertyManager)

  /** The required environment for the BackfillMergeBatchProcessor.
    */
  type Environment = MergeServiceClient & SinkEntityManager & SinkPropertyManager

  /** The ZLayer that creates the MergeProcessor.
    */
  val layer: ZLayer[Environment, Nothing, BackfillApplyBatchProcessor] =
    ZLayer {
      for
        jdbcConsumer    <- ZIO.service[MergeServiceClient]
        entityManager   <- ZIO.service[SinkEntityManager]
        propertyManager <- ZIO.service[SinkPropertyManager]
      yield BackfillApplyBatchProcessor(jdbcConsumer, entityManager, propertyManager)
    }
