package com.sneaksanddata.arcane.framework
package services.base

import models.batches.{MergeableBatch, StagedBackfillBatch, StagedBatch, StagedVersionedBatch}

import zio.Task

/** Result of a batch disposal
  */
case class BatchDisposeResult(isSuccess: Boolean)

/** A service client that disposes of data batches.
  */
trait DisposeServiceClient:
  
    /** Disposes of a batch.
    */
  def disposeBatch(batch: StagedBatch): Task[BatchDisposeResult]
