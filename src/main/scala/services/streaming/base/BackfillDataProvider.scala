package com.sneaksanddata.arcane.framework
package services.streaming.base

import zio.stream.ZStream

/**
 * A trait that represents a backfill data provider.
 */
trait BackfillDataProvider[DataBatchType]:

  /**
   * Provides the backfill data.
   *
   * @return A task that represents the backfill data.
   */
  def requestBackfill: ZStream[Any, Throwable, DataBatchType]
