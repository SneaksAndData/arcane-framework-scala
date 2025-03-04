package com.sneaksanddata.arcane.framework
package services.base

import models.ArcaneSchema
import services.consumers.StagedBatch

import zio.Task


/**
 * The result of applying a batch.
 */
type BatchApplicationResult = Boolean


/**
 * The result of disposing of a batch.
 */
class BatchDisposeResult

/**
 * A service client that merges data batches.
 */
trait MergeServiceClient:

  type Batch = StagedBatch

  /**
   * Applies a batch to the target table.
   *
   * @param batch The batch to apply.
   * @return The result of applying the batch.
   */
  def applyBatch(batch: Batch): Task[BatchApplicationResult]

  /**
   * Disposes of a batch.
   *
   * @param batch The batch to dispose.
   * @return The result of disposing of the batch.
   */
  def disposeBatch(batch: Batch): Task[BatchDisposeResult]
