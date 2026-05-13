package com.sneaksanddata.arcane.framework
package services.streaming.batching

import models.batches.{MergeableBatch, StagedBatch, StagedVersionedBatch}
import models.schemas.ArcaneSchema

import zio.Task

/** Factory for staged batches, source-specific
  */
trait StagedBatchFactory:
  type OutputBatch <: StagedVersionedBatch & MergeableBatch
  type WatermarkBatch <: StagedVersionedBatch & MergeableBatch

  /** A batch containing data to merge
    */
  def createDataBatch(stagedTableName: String, targetTableName: String, batchSchema: ArcaneSchema): Task[OutputBatch]

  /** A batch containing watermark to apply to target
    * @return
    */
  def createWatermarkBatch(targetTableName: String, watermark: String): Task[WatermarkBatch]
