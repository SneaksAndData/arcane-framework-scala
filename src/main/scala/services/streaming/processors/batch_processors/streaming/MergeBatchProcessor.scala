package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.streaming

import logging.ZIOLogAnnotations.*
import models.settings.*
import services.base.MergeServiceClient
import services.merging.{JdbcMergeServiceClient, JdbcTableManager}
import services.streaming.base.StagedBatchProcessor

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/**
 * Processor that merges data into a target table.
 */
class MergeBatchProcessor(mergeServiceClient: MergeServiceClient, tableManager: JdbcTableManager, targetTableSettings: TargetTableSettings)
  extends StagedBatchProcessor:

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] =
    ZPipeline.mapZIO(batchesSet =>
      for _ <- zlog(s"Applying batch set with index ${batchesSet.batchIndex}")
          _ <- ZIO.foreach(batchesSet.groupedBySchema)(batch => tableManager.migrateSchema(batch.schema, batch.targetTableName))
          _ <- ZIO.foreach(batchesSet.groupedBySchema)(batch => mergeServiceClient.applyBatch(batch))
      
          _ <- tableManager.optimizeTable(batchesSet.getOptimizationRequest(targetTableSettings.maintenanceSettings.targetOptimizeSettings))
          _ <- tableManager.expireSnapshots(batchesSet.getSnapshotExpirationRequest(targetTableSettings.maintenanceSettings.targetSnapshotExpirationSettings))
          _ <- tableManager.expireOrphanFiles(batchesSet.getOrphanFileExpirationRequest(targetTableSettings.maintenanceSettings.targetOrphanFilesExpirationSettings))
      yield batchesSet
    )

object MergeBatchProcessor:

  /**
   * Factory method to create MergeProcessor
   *
   * @param mergeServiceClient The JDBC consumer.
   * @param tableManager The table manager.
   * @param targetTableSettings The target table settings.
   * @return The initialized MergeProcessor instance
   */
  def apply(mergeServiceClient: MergeServiceClient, tableManager: JdbcTableManager, targetTableSettings: TargetTableSettings): MergeBatchProcessor =
    new MergeBatchProcessor(mergeServiceClient, tableManager, targetTableSettings)

  /**
   * The required environment for the MergeBatchProcessor.
   */
  type Environment = MergeServiceClient
    & TargetTableSettings
    & JdbcTableManager

  /**
   * The ZLayer that creates the MergeProcessor.
   */
  val layer: ZLayer[Environment, Nothing, MergeBatchProcessor] =
    ZLayer {
      for
        jdbcConsumer <- ZIO.service[MergeServiceClient]
        targetTableSettings <- ZIO.service[TargetTableSettings]
        tableManager <- ZIO.service[JdbcTableManager]
      yield MergeBatchProcessor(jdbcConsumer, tableManager, targetTableSettings)
    }
