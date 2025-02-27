package com.sneaksanddata.arcane.framework
package services.streaming.base

import services.streaming.processors.transformers.IndexedStagedBatches

import zio.stream.ZPipeline

/**
 * A trait that represents a batch processor.
 */
trait StagedBatchProcessor {
  
  type BatchType <: IndexedStagedBatches

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  def process: ZPipeline[Any, Throwable, BatchType, BatchType]
}
