package com.sneaksanddata.arcane.framework
package services.streaming.base

import zio.stream.ZStream

trait StreamDataProvider:

  type StreamElementType

  def stream: ZStream[Any, Throwable, StreamElementType]
