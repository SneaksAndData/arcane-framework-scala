package com.sneaksanddata.arcane.framework
package services.streaming.base

import zio.Task

/** A trait that represents a backfill data provider.
  */
trait BackfillStreamingMergeDataProvider:

  /** Provides the backfill data.
    *
    * @return
    *   A task that represents the backfill data.
    */
  def requestBackfill: Task[Unit]
