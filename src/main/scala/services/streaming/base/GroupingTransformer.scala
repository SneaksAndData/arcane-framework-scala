package com.sneaksanddata.arcane.framework
package services.streaming.base

import zio.Chunk
import zio.stream.ZPipeline

 /**
   * A trait that represents a stream transformer that transforms a stream of elements into a stream of grouped elements.
  */
trait GroupingTransformer:
  
 /**
  * Processes the incoming data.
  *
  * @return ZPipeline (stream source for the stream graph).
  */
  def process[Element: MetadataEnrichedRowStreamElement]: ZPipeline[Any, Throwable, Element, Chunk[Element]]
