package com.sneaksanddata.arcane.framework
package services.streaming.base

import zio.stream.ZStream

/**
 * Provides the complete data stream for the streaming process including all the stages and services
 * except the sink and lifetime service.
 */
trait StreamingGraphBuilder:

  /**
   * The type of the processed batch.
   */
  type ProcessedBatch

  /**
   * Produces the stream of processed batches.
   */
  def produce: ZStream[Any, Throwable, ProcessedBatch]
