package com.sneaksanddata.arcane.framework
package services.streaming.base

import zio.stream.ZPipeline

/** A trait that represents a row processor.
  */
trait RowProcessor:

  type Element = GroupingTransformer#Element

  /** Processes the incoming data.
    *
    * @return
    *   ZPipeline (stream source for the stream graph).
    */
  def process: ZPipeline[Any, Throwable, Element, Element]
