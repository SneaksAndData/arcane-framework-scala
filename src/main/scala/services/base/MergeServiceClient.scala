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
 * The result of applying a batch.
 */
class BatchArchivationResult

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
   * Applies a batch to the archive table.
   *
   * @param batch         The batch to archive.
   * @param actualSchema  The actual schema of the batch.
   * @return The result of archiving the batch.
   */
  def archiveBatch(batch: Batch, actualSchema: ArcaneSchema): Task[BatchArchivationResult]

  /**
   * Disposes of a batch.
   *
   * @param batch The batch to dispose.
   * @return The result of disposing of the batch.
   */
  def disposeBatch(batch: Batch): Task[BatchDisposeResult]
