package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.streaming

import logging.ZIOLogAnnotations.*
import models.settings.*
import services.base.MergeServiceClient
import services.merging.JdbcTableManager
import services.metrics.DeclaredMetrics
import services.metrics.DeclaredMetrics._
import services.streaming.base.StagedBatchProcessor

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/** Processor that merges data into a target table.
  */
class MergeBatchProcessor(
    mergeServiceClient: MergeServiceClient,
    tableManager: JdbcTableManager,
    targetTableSettings: TargetTableSettings,
    declaredMetrics: DeclaredMetrics
) extends StagedBatchProcessor:

  /** Processes the incoming data.
    *
    * @return
    *   ZPipeline (stream source for the stream graph).
    */
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] =
    ZPipeline.mapZIO(batchesSet =>
      (for
        _ <- zlog("Applying batch set with index %s", batchesSet.batchIndex.toString)
        _ <- ZIO.foreach(batchesSet.groupedBySchema)(batch =>
          tableManager.migrateSchema(batch.schema, batch.targetTableName)
        )
        _ <- ZIO
          .foreach(batchesSet.groupedBySchema)(batch => mergeServiceClient.applyBatch(batch))

        _ <- tableManager.optimizeTable(
          batchesSet.getOptimizationRequest(targetTableSettings.maintenanceSettings.targetOptimizeSettings)
        )
        _ <- tableManager.expireSnapshots(
          batchesSet.getSnapshotExpirationRequest(
            targetTableSettings.maintenanceSettings.targetSnapshotExpirationSettings
          )
        )
        _ <- tableManager.expireOrphanFiles(
          batchesSet.getOrphanFileExpirationRequest(
            targetTableSettings.maintenanceSettings.targetOrphanFilesExpirationSettings
          )
        )
      yield batchesSet).gaugeDuration(declaredMetrics.batchMergeStageDuration)
    )

object MergeBatchProcessor:

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
  def apply(
      mergeServiceClient: MergeServiceClient,
      tableManager: JdbcTableManager,
      targetTableSettings: TargetTableSettings,
      declaredMetrics: DeclaredMetrics
  ): MergeBatchProcessor =
    new MergeBatchProcessor(mergeServiceClient, tableManager, targetTableSettings, declaredMetrics)

  /** The required environment for the MergeBatchProcessor.
    */
  type Environment = MergeServiceClient & TargetTableSettings & JdbcTableManager & DeclaredMetrics

  /** The ZLayer that creates the MergeProcessor.
    */
  val layer: ZLayer[Environment, Nothing, MergeBatchProcessor] =
    ZLayer {
      for
        jdbcConsumer        <- ZIO.service[MergeServiceClient]
        targetTableSettings <- ZIO.service[TargetTableSettings]
        tableManager        <- ZIO.service[JdbcTableManager]
        declaredMetrics     <- ZIO.service[DeclaredMetrics]
      yield MergeBatchProcessor(jdbcConsumer, tableManager, targetTableSettings, declaredMetrics)
    }
