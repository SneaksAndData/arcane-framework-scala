package com.sneaksanddata.arcane.framework
package services.base

import models.batches.StagedBatch

import zio.Task

/** Result of a batch disposal
  */
case class BatchDisposeResult(isSuccess: Boolean)

/** A service client that disposes of data batches.
  */
trait DisposeServiceClient:
  type Batch = StagedBatch

  /** Disposes of a batch.
    *
    * @param batch
    *   The batch to dispose.
    * @return
    *   The result of disposing of the batch.
    */
  def disposeBatch(batch: Batch): Task[BatchDisposeResult]
