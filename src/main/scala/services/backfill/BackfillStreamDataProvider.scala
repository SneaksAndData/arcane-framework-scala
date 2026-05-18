package com.sneaksanddata.arcane.framework
package services.backfill

import models.sharding.BootstrappedShard
import services.streaming.base.JsonWatermark

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
  def backfillStream: Task[(stream: ZStream[Any, Throwable, BootstrappedShard], watermark: JsonWatermark)]
