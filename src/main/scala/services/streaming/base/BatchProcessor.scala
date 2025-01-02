package com.sneaksanddata.arcane.framework
package services.streaming.base

import zio.stream.ZPipeline

/**
 * A trait that represents a batch processor.
 * @tparam IncomingType The type of the incoming data.
 */
trait BatchProcessor[IncomingType, OutgoingType] {

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  def process: ZPipeline[Any, Throwable, IncomingType, OutgoingType]
}
