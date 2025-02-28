package com.sneaksanddata.arcane.framework
package services.streaming.base

import models.settings.{OptimizeSettings, OrphanFilesExpirationSettings, SnapshotExpirationSettings, TableMaintenanceSettings}
import services.base.{BatchOptimizationResult, TableManager}
import services.merging.models.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import services.streaming.processors.transformers.IndexedStagedBatches

import com.sneaksanddata.arcane.framework.services.merging.JdbcTableManager
import zio.stream.ZPipeline
import zio.{Task, ZIO}

trait OptimizationRequestConvertable:
  def getOptimizationRequest(settings: OptimizeSettings): JdbcOptimizationRequest

trait SnapshotExpirationRequestConvertable:
  def getSnapshotExpirationRequest(settings: SnapshotExpirationSettings): JdbcSnapshotExpirationRequest

trait OrphanFilesExpirationRequestConvertable:
  def getOrphanFileExpirationRequest(settings: OrphanFilesExpirationSettings): JdbcOrphanFilesExpirationRequest

/**
 * A trait that represents a batch processor.
 */
trait StagedBatchProcessor {
  
  type BatchType = IndexedStagedBatches
    & SnapshotExpirationRequestConvertable
    & OrphanFilesExpirationRequestConvertable
    & OptimizationRequestConvertable


  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  def process: ZPipeline[Any, Throwable, BatchType, BatchType]

  type MaintenanceOperation[T] = (BatchType, T) => Task[BatchOptimizationResult];

  type MaintenanceOperationResult = Task[BatchOptimizationResult|Unit]

  protected def runMaintenanceTasks(batchesSet: BatchType, maintenanceSettings: TableMaintenanceSettings, tableManager: JdbcTableManager): Task[Unit] =
    for
      _ <- runMaintenance(batchesSet, maintenanceSettings.targetOptimizeSettings) {
        (batchesSet, settings) => tableManager.optimizeTable(batchesSet.getOptimizationRequest(settings))
      }
      _ <- runMaintenance(batchesSet, maintenanceSettings.targetSnapshotExpirationSettings) {
        (batchesSet, settings) => tableManager.expireSnapshots(batchesSet.getSnapshotExpirationRequest(settings))
      }
      _ <- runMaintenance(batchesSet, maintenanceSettings.targetOrphanFilesExpirationSettings) {
        (batchesSet, settings) => tableManager.expireOrphanFiles(batchesSet.getOrphanFileExpirationRequest(settings))
      }
    yield ()

  protected def runMaintenance[T](batchSet: BatchType, settings: Option[T])(action: MaintenanceOperation[T]): MaintenanceOperationResult  =
    settings.map(s => action(batchSet, s)).getOrElse(ZIO.unit)
}
