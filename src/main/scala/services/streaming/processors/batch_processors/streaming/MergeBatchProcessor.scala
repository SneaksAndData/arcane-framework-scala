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
class MergeBatchProcessor(mergeServiceClient: JdbcMergeServiceClient, targetTableSettings: TargetTableSettings)
  extends StagedBatchProcessor:

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] =
    ZPipeline.mapZIO(batchesSet =>
      for _ <- zlog(s"Applying batch set with index ${batchesSet.batchIndex}")
      
          _ <- ZIO.foreach(Seq(batchesSet.getOptimizationRequest(targetTableSettings.maintenanceSettings.targetOptimizeSettings)))(req => mergeServiceClient.optimizeTable(req).orDieWith(e => Throwable(s"Failed to optimize while executing maintenance for batch ${batchesSet.batchIndex}", e)))

          _ <- ZIO.foreach(batchesSet.groupedBySchema)(batch => mergeServiceClient.migrateSchema(batch.schema, batch.targetTableName))
          _ <- ZIO.foreach(batchesSet.groupedBySchema)(batch => mergeServiceClient.applyBatch(batch))
      
          _ <- mergeServiceClient.expireSnapshots(batchesSet.getSnapshotExpirationRequest(targetTableSettings.maintenanceSettings.targetSnapshotExpirationSettings)).orDieWith(e => Throwable(s"Failed expire snapshots while executing maintenance for batch ${batchesSet.batchIndex}", e))
      
          _ <- mergeServiceClient.expireOrphanFiles(batchesSet.getOrphanFileExpirationRequest(targetTableSettings.maintenanceSettings.targetOrphanFilesExpirationSettings)).orDieWith(e => Throwable(s"Failed to remove orphan files while executing maintenance for batch ${batchesSet.batchIndex}", e))
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
  def apply(mergeServiceClient: JdbcMergeServiceClient, targetTableSettings: TargetTableSettings): MergeBatchProcessor =
    new MergeBatchProcessor(mergeServiceClient, targetTableSettings)

  /**
   * The required environment for the MergeBatchProcessor.
   */
  type Environment = JdbcMergeServiceClient & TargetTableSettings

  /**
   * The ZLayer that creates the MergeProcessor.
   */
  val layer: ZLayer[Environment, Nothing, MergeBatchProcessor] =
    ZLayer {
      for
        jdbcConsumer <- ZIO.service[JdbcMergeServiceClient]
        targetTableSettings <- ZIO.service[TargetTableSettings]
      yield MergeBatchProcessor(jdbcConsumer, targetTableSettings)
    }
