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
  def getOptimizationRequest(settings: Option[OptimizeSettings]): Option[JdbcOptimizationRequest]

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
  def getSnapshotExpirationRequest(settings: Option[SnapshotExpirationSettings]): Option[JdbcSnapshotExpirationRequest]

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
  def getOrphanFileExpirationRequest(settings: Option[OrphanFilesExpirationSettings]): Option[JdbcOrphanFilesExpirationRequest]

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
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  def process: ZPipeline[Any, Throwable, BatchType, BatchType]
