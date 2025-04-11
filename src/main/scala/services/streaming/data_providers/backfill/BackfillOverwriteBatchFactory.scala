package com.sneaksanddata.arcane.framework
package services.streaming.data_providers.backfill

import services.consumers.StagedBackfillOverwriteBatch

import zio.Task

/**
 * Creates backfill overwrite batches.
 * The streaming plugins must implement this interface to be able to produce backfill batches needed for the
 * backfill overwrite process.
 */
trait BackfillOverwriteBatchFactory:
  
  /**
   * Creates a backfill batch.
   *
   * @param intermediateTableName The name of the intermediate table.
   * @return A task that represents the backfill batch.
   */
  def createBackfillBatch: Task[StagedBackfillOverwriteBatch]
