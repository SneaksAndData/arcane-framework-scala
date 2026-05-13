package com.sneaksanddata.arcane.framework
package services.backfill

import zio.Task

/** A trait that represents a backfill data provider.
  */
trait BackfillStreamDataProvider:

  /** Provides the backfill data.
    *
    * @return
    *   A task that represents the backfill data.
    */
  def backfill: Task[Unit]
