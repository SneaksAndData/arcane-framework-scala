package com.sneaksanddata.arcane.framework
package services.streaming.base

import models.batches.{MergeableBatch, StagedVersionedBatch}
import models.maintenance.{JdbcAnalyzeRequest, JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import models.schemas.ArcaneSchema
import models.settings.sink.{AnalyzeSettings, OptimizeSettings, OrphanFilesExpirationSettings, SnapshotExpirationSettings}

import zio.stream.ZPipeline

/** A trait that represents a batch processor.
  */
trait StagedBatchProcessor extends StreamingBatchProcessor:

  /** @inheritdoc
    */
  override type BatchType = StagedVersionedBatch & MergeableBatch

  /** Processes the incoming data.
    *
    * @return
    *   ZPipeline (stream source for the stream graph).
    */
  def process: ZPipeline[Any, Throwable, BatchType, BatchType]
