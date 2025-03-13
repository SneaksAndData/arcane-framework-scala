package com.sneaksanddata.arcane.framework
package services.streaming.base

import zio.stream.ZSink

/**
 * A trait that represents a grouped data batch consumer.
 * @tparam ConsumableBatch The type of the consumable batch.
 */
trait BatchConsumer[ConsumableBatch]:

  /**
   * Returns the sink that consumes the batch.
   *
   * @return ZSink (stream sink for the stream graph).
   */
  def consume: ZSink[Any, Throwable, ConsumableBatch, Any, Unit]
