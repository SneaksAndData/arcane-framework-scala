package com.sneaksanddata.arcane.framework
package services.backfill

import models.batches.StagedBackfillOverwriteBatch

import zio.Task

/** A trait that represents a backfill data provider.
  */
trait BackfillStreamingOverwriteDataProvider:

  type BatchType = StagedBackfillOverwriteBatch | Unit

  /** Provides the backfill data.
    *
    * @return
    *   A task that represents the backfill data.
    */
  def requestBackfill: Task[BatchType]
