package com.sneaksanddata.arcane.framework
package services.streaming.base

import models.DataRow

import zio.stream.ZPipeline

/**
  * A trait that represents a row processor.
 */
trait RowProcessor:

  type Element = DataRow|Any
  
  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  def process: ZPipeline[Any, Throwable, Element, Element]
