package com.sneaksanddata.arcane.framework
package services.streaming.base

import services.streaming.processors.transformers.IndexedStagedBatches

import zio.stream.ZStream

/** Provides the complete data stream for the streaming process including all the stages and services except the sink
  * and lifetime service.
  */
trait BackfillSubStream:

  /** The type of the processed batch.
    */
  type ProcessedBatch <: IndexedStagedBatches

  /** Produces the stream of processed batches.
    * @param hookManager
    *   The hook manager.
    */
  def produce(hookManager: HookManager): ZStream[Any, Throwable, ProcessedBatch]
