package com.sneaksanddata.arcane.framework
package services.streaming.base

import models.batches.StagedBatch

import zio.stream.ZStream

/** Provides the complete data stream for the streaming process including all the stages and services except the sink
  * and lifetime service.
  */
trait BackfillSubStream:

  /** The type of the processed batch.
    */
  type ProcessedBatch <: StagedBatch

  /** Produces the stream of processed batches.
    */
  def produce(): ZStream[Any, Throwable, ProcessedBatch]
