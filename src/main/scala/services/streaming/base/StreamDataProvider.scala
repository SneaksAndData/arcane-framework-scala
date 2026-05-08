package com.sneaksanddata.arcane.framework
package services.streaming.base

import zio.stream.ZStream

/** Provides data from the data source for Arcane stream.
  */
trait StreamDataProvider:

  /** Returns the stream of elements.
    */
  def stream: ZStream[Any, Throwable, StructuredZStream]

