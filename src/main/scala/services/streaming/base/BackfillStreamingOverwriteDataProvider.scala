package com.sneaksanddata.arcane.framework
package services.streaming.base

import services.consumers.StagedBackfillOverwriteBatch

import zio.Task

/**
 * A trait that represents a backfill data provider.
 */
trait BackfillStreamingOverwriteDataProvider:

  /**
   * Provides the backfill data.
   *
   * @return A task that represents the backfill data.
   */
  def requestBackfill: Task[StagedBackfillOverwriteBatch]