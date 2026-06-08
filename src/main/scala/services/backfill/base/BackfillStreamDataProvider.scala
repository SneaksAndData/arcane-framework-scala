package com.sneaksanddata.arcane.framework
package services.backfill.base

import services.streaming.base.StructuredZStream

import zio.stream.ZStream

trait BackfillStreamDataProvider:
  /** Returns the stream of elements.
    */
  def stream: ZStream[Any, Throwable, StructuredZStream]
