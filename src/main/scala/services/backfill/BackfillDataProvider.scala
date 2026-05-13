package com.sneaksanddata.arcane.framework
package services.backfill

import com.sneaksanddata.arcane.framework.services.streaming.base.StructuredZStream
import zio.stream.ZStream

/** A trait that represents a backfill data provider.
  */
trait BackfillDataProvider:

  /** Provides the backfill data.
    *
    * @return
    *   A task that represents the backfill data.
    */
  def requestBackfill: ZStream[Any, Throwable, StructuredZStream]
