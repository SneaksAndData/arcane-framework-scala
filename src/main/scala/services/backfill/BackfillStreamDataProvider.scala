package com.sneaksanddata.arcane.framework
package services.backfill

import com.sneaksanddata.arcane.framework.models.sharding.SourceShard
import zio.Task
import zio.stream.ZStream

/** A trait that represents a backfill data provider.
  */
trait BackfillStreamDataProvider:

  /** Provides the backfill data.
    *
    * @return
    *   A task that represents the backfill data.
    */
  def backfillStream: ZStream[Any, Throwable, SourceShard]
