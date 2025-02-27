package com.sneaksanddata.arcane.framework
package services.streaming.processors

import logging.ZIOLogAnnotations.*
import models.settings.{OptimizeSettings, OrphanFilesExpirationSettings, SnapshotExpirationSettings, TableMaintenanceSettings, TargetTableSettings}
import services.base.{BatchOptimizationResult, MergeServiceClient}
import services.merging.JdbcTableManager
import services.merging.models.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import services.streaming.base.StagedBatchProcessor
import services.streaming.processors.transformers.IndexedStagedBatches

import zio.stream.ZPipeline
import zio.{Task, ZIO, ZLayer}

trait OptimizationRequestConvertable:
  def getOptimizationRequest(settings: OptimizeSettings): JdbcOptimizationRequest

trait SnapshotExpirationRequestConvertable:
  def getSnapshotExpirationRequest(settings: SnapshotExpirationSettings): JdbcSnapshotExpirationRequest

trait OrphanFilesExpirationRequestConvertable:
  def getOrphanFileExpirationRequest(settings: OrphanFilesExpirationSettings): JdbcOrphanFilesExpirationRequest

/**
 * Processor that merges data into a target table.
 */
class MergeBatchProcessor(mergeServiceClient: MergeServiceClient, tableManager: JdbcTableManager, targetTableSettings: TargetTableSettings)
  extends StagedBatchProcessor:

  type BatchType = IndexedStagedBatches
    & SnapshotExpirationRequestConvertable
    & OrphanFilesExpirationRequestConvertable
    & OptimizationRequestConvertable

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] =
    ZPipeline.mapZIO(batchesSet =>
      for _ <- zlog(s"Applying batch with index ${batchesSet.batchIndex}")
          _ <- ZIO.foreach(batchesSet.groupedBySchema)(batch => tableManager.migrateSchema(batch.schema, batch.targetTableName))
          _ <- ZIO.foreach(batchesSet.groupedBySchema)(batch => mergeServiceClient.applyBatch(batch))

          _ <- runMaintenance(batchesSet, targetTableSettings.maintenanceSettings.targetOptimizeSettings) {
              (batchesSet, settings) => tableManager.optimizeTable(batchesSet.getOptimizationRequest(settings))
            }
          _ <- runMaintenance(batchesSet, targetTableSettings.maintenanceSettings.targetSnapshotExpirationSettings) {
            (batchesSet, settings) => tableManager.expireSnapshots(batchesSet.getSnapshotExpirationRequest(settings))
          }
          _ <- runMaintenance(batchesSet, targetTableSettings.maintenanceSettings.targetOrphanFilesExpirationSettings) {
            (batchesSet, settings) => tableManager.expireOrphanFiles(batchesSet.getOrphanFileExpirationRequest(settings))
          }
      yield batchesSet
    )

  private def runMaintenance[T, Result](batchSet: BatchType, maintenanceSettings: Option[T])(action: (BatchType, T) => Task[BatchOptimizationResult]): Task[BatchOptimizationResult|Unit] =
    maintenanceSettings.map(s => action(batchSet, s)).getOrElse(ZIO.unit)

object MergeBatchProcessor:

  /**
   * Factory method to create MergeProcessor
   *
   * @param jdbcConsumer The JDBC consumer.
   * @return The initialized MergeProcessor instance
   */
  def apply(mergeServiceClient: MergeServiceClient, tableManager: JdbcTableManager, targetTableSettings: TargetTableSettings): MergeBatchProcessor =
    new MergeBatchProcessor(mergeServiceClient, tableManager, targetTableSettings)

  /**
   * The required environment for the MergeProcessor.
   */
  type Environment = MergeServiceClient & JdbcTableManager & TargetTableSettings

  /**
   * The ZLayer that creates the MergeProcessor.
   */
  val layer: ZLayer[Environment, Nothing, MergeBatchProcessor] =
    ZLayer {
      for
        jdbcConsumer <- ZIO.service[MergeServiceClient]
        parallelismSettings <- ZIO.service[JdbcTableManager]
        targetTableSettings <- ZIO.service[TargetTableSettings]
      yield MergeBatchProcessor(jdbcConsumer, parallelismSettings, targetTableSettings)
    }
