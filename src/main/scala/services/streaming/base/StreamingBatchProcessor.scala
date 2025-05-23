package com.sneaksanddata.arcane.framework
package services.streaming.base

import zio.stream.ZPipeline

/** Represents a streaming stage that processes batches.
  */
trait StreamingBatchProcessor:

  /** The type of the batch.
    */
  type BatchType

  /** Processes the incoming data.
    *
    * @return
    *   ZPipeline (stream source for the stream graph).
    */
  def process: ZPipeline[Any, Throwable, BatchType, BatchType]
