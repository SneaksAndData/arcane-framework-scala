package com.sneaksanddata.arcane.framework
package services.streaming.base

import services.streaming.processors.transformers.StagingProcessor

import zio.stream.ZStream

trait StreamingGraphBuilder:
  
  type ProcessedBatch
  
  def produce: ZStream[Any, Throwable, ProcessedBatch]
