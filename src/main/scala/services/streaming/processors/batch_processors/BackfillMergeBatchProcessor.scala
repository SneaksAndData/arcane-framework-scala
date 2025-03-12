package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors

import logging.ZIOLogAnnotations.*
import models.settings.*
import services.base.MergeServiceClient
import services.merging.JdbcTableManager
import services.streaming.base.{BatchProcessor, OptimizationRequestConvertable, OrphanFilesExpirationRequestConvertable, SnapshotExpirationRequestConvertable, StagedBatchProcessor}

import com.sneaksanddata.arcane.framework.services.consumers.{MergeableBatch, StagedBackfillBatch}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.IndexedStagedBatches
import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/**
 * Processor that merges data into a target table.
 */
class BackfillMergeBatchProcessor(mergeServiceClient: MergeServiceClient, tableManager: JdbcTableManager, targetTableSettings: TargetTableSettings)
  extends BatchProcessor:

  override type BatchType = StagedBackfillBatch
  
  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] =
    ZPipeline.mapZIO(batch =>
      for _ <- zlog(s"Applying backfill batch to ${batch.targetTableName}")
          _ <- tableManager.migrateSchema(batch.schema, batch.targetTableName)
          _ <-  mergeServiceClient.applyBatch(batch)
      yield  batch
    )

object BackfillMergeBatchProcessor:

  /**
   * Factory method to create MergeProcessor
   *
   * @param mergeServiceClient The JDBC consumer.
   * @param tableManager The table manager.
   * @param targetTableSettings The target table settings.
   * @return The initialized MergeProcessor instance
   */
  def apply(mergeServiceClient: MergeServiceClient, tableManager: JdbcTableManager, targetTableSettings: TargetTableSettings): BackfillMergeBatchProcessor =
    new BackfillMergeBatchProcessor(mergeServiceClient, tableManager, targetTableSettings)

  /**
   * The required environment for the BackfillMergeBatchProcessor.
   */
  type Environment = MergeServiceClient & JdbcTableManager & TargetTableSettings

  /**
   * The ZLayer that creates the MergeProcessor.
   */
  val layer: ZLayer[Environment, Nothing, BackfillMergeBatchProcessor] =
    ZLayer {
      for
        jdbcConsumer <- ZIO.service[MergeServiceClient]
        parallelismSettings <- ZIO.service[JdbcTableManager]
        targetTableSettings <- ZIO.service[TargetTableSettings]
      yield BackfillMergeBatchProcessor(jdbcConsumer, parallelismSettings, targetTableSettings)
    }
