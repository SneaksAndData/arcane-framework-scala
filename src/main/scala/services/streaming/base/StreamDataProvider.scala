package com.sneaksanddata.arcane.framework
package services.streaming.base

import zio.Task
import zio.stream.ZStream

import java.time.Duration

trait StreamDataProvider[StreamElementType: MetadataEnrichedRowStreamElement]:
  
  def stream: ZStream[Any, Throwable, StreamElementType]
