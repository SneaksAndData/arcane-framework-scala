package com.sneaksanddata.arcane.framework
package services.streaming.base

import org.apache.hc.core5.annotation.Obsolete
import zio.stream.ZPipeline

/**
 * A trait that represents a batch processor.
 * @tparam IncomingType The type of the incoming data.
 */
@Obsolete
trait BatchProcessor[IncomingType, OutgoingType] {

  /**
   * The type of the batch.
   */
  type BatchType

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  def process: ZPipeline[Any, Throwable, IncomingType, OutgoingType]
}
