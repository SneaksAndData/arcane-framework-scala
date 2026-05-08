package com.sneaksanddata.arcane.framework
package services.base

import models.batches.StagedBatch

import zio.Task

/** The result of applying a batch.
  */
type BatchApplicationResult = Boolean

/** A service client that merges data batches.
  */
trait MergeServiceClient:

  type Batch = StagedBatch

  /** Applies a batch to the target table.
    *
    * @param batch
    *   The batch to apply.
    * @return
    *   The result of applying the batch.
    */
  def applyBatch(batch: Batch): Task[BatchApplicationResult]
