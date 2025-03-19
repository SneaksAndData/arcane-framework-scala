package com.sneaksanddata.arcane.framework
package services.streaming.base

import models.settings.{OptimizeSettings, OrphanFilesExpirationSettings, SnapshotExpirationSettings, TableMaintenanceSettings}
import services.base.BatchOptimizationResult
import services.merging.JdbcTableManager
import services.merging.models.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import services.streaming.processors.transformers.IndexedStagedBatches

import zio.stream.ZPipeline
import zio.{Task, ZIO}



/**
 * A trait that represents a batch that can be converted to an optimization request.
 */
trait OptimizationRequestConvertable:

  /**
   * Gets the optimization request.
   *
   * @param settings The optimization settings.
   * @return The optimization request.
   */
  def getOptimizationRequest(settings: OptimizeSettings): JdbcOptimizationRequest

/**
 * A trait that represents a batch that can be converted to a snapshot expiration request.
 */
trait SnapshotExpirationRequestConvertable:

  /**
   * Gets the snapshot expiration request.
   *
   * @param settings The snapshot expiration settings.
   * @return The snapshot expiration request.
   */
  def getSnapshotExpirationRequest(settings: SnapshotExpirationSettings): JdbcSnapshotExpirationRequest

/**
 * A trait that represents a batch that can be converted to an orphan files expiration request.
 */
trait OrphanFilesExpirationRequestConvertable:
  /**
   * Gets the orphan files expiration request.
   *
   * @param settings The orphan files expiration settings.
   * @return The orphan files expiration request.
   */
  def getOrphanFileExpirationRequest(settings: OrphanFilesExpirationSettings): JdbcOrphanFilesExpirationRequest

/**
 * A trait that represents a batch processor.
 */
trait StagedBatchProcessor extends StreamingBatchProcessor:

  /**
   * @inheritdoc
   */
  override type BatchType = IndexedStagedBatches
    & SnapshotExpirationRequestConvertable
    & OrphanFilesExpirationRequestConvertable
    & OptimizationRequestConvertable

  /**
   * Represents a maintenance operation.
   * @tparam T type of the maintenance settings
   */
  private type MaintenanceOperation[T] = (BatchType, T) => Task[BatchOptimizationResult]

  /**
   * Represents a maintenance operation result. Can be either a BatchOptimizationResult if the table maintenance request
   * was created or Unit if the maintenance settings were not provided and no operation was performed.
   * @tparam T type of the maintenance settings
   */
  private type MaintenanceOperationResult = Task[BatchOptimizationResult|Unit]

  /**
   * Runs the maintenance tasks.
   *
   * @param batchesSet The batch set.
   * @param maintenanceSettings The maintenance settings.
   * @param tableManager The table manager.
   * @return The result of the maintenance tasks.
   */
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

  private def runMaintenance[T](batchSet: BatchType, settings: Option[T])(action: MaintenanceOperation[T]): MaintenanceOperationResult  =
    settings.map(s => action(batchSet, s)).getOrElse(ZIO.unit)
