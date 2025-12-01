package com.sneaksanddata.arcane.framework
package services.streaming.base

import models.settings.{AnalyzeSettings, OptimizeSettings, OrphanFilesExpirationSettings, SnapshotExpirationSettings}
import services.merging.maintenance.{
  JdbcAnalyzeRequest,
  JdbcOptimizationRequest,
  JdbcOrphanFilesExpirationRequest,
  JdbcSnapshotExpirationRequest
}
import services.streaming.processors.transformers.IndexedStagedBatches

import zio.stream.ZPipeline

/** A trait that represents a batch that can be converted to an optimization request.
  */
trait OptimizationRequestConvertable:

  /** Gets the optimization request.
    *
    * @param settings
    *   The optimization settings.
    * @return
    *   The optimization request.
    */
  def getOptimizationRequest(settings: Option[OptimizeSettings]): Option[JdbcOptimizationRequest]

/** A trait that represents a batch that can be converted to a snapshot expiration request.
  */
trait SnapshotExpirationRequestConvertable:

  /** Gets the snapshot expiration request.
    *
    * @param settings
    *   The snapshot expiration settings.
    * @return
    *   The snapshot expiration request.
    */
  def getSnapshotExpirationRequest(settings: Option[SnapshotExpirationSettings]): Option[JdbcSnapshotExpirationRequest]

/** A trait that represents a batch that can be converted to an orphan files expiration request.
  */
trait OrphanFilesExpirationRequestConvertable:
  /** Gets the orphan files expiration request.
    *
    * @param settings
    *   The orphan files expiration settings.
    * @return
    *   The orphan files expiration request.
    */
  def getOrphanFileExpirationRequest(
      settings: Option[OrphanFilesExpirationSettings]
  ): Option[JdbcOrphanFilesExpirationRequest]

/** A trait that represents a batch that can be converted to an orphan files expiration request.
  */
trait AnalyzeRequestConvertable:
  /** Gets the analyze request.
    *
    * @param settings
    *   The analyze settings.
    * @return
    *   The analyze request.
    */
  def getAnalyzeRequest(settings: Option[AnalyzeSettings]): Option[JdbcAnalyzeRequest]

/** A trait that represents a batch processor.
  */
trait StagedBatchProcessor extends StreamingBatchProcessor:

  /** @inheritdoc
    */
  override type BatchType = IndexedStagedBatches & SnapshotExpirationRequestConvertable &
    OrphanFilesExpirationRequestConvertable & OptimizationRequestConvertable & AnalyzeRequestConvertable

  /** Processes the incoming data.
    *
    * @return
    *   ZPipeline (stream source for the stream graph).
    */
  def process: ZPipeline[Any, Throwable, BatchType, BatchType]
