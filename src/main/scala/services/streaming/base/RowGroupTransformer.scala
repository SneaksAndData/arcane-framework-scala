package com.sneaksanddata.arcane.framework
package services.streaming.base

import models.DataRow

import services.consumers.{MergeableBatch, StagedBatch, StagedVersionedBatch}
import services.streaming.processors.transformers.IndexedStagedBatches
import zio.Chunk
import zio.stream.ZPipeline



trait ToInFlightBatch[T]:
  /**
   * Converts the staged batches to the outgoing type.
   *
   * @param batches The staged batches.
   * @param batchIndex The batch index.
   * @param others The other elements.
   * @return The outgoing type.
   */
  extension (batches: Iterable[StagedVersionedBatch]) def toBatch[Element: MetadataEnrichedRowStreamElement](batchIndex: Long, others: Chunk[Element]): T

/**
  * A trait that represents a row processor.
 */
trait RowGroupTransformer:

  type OutgoingElement <: IndexedStagedBatches
  
  type ToInFlightBatch = (Iterable[StagedVersionedBatch & MergeableBatch], Long, Chunk[Any]) => OutgoingElement
  
  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  def process[IncomingElement: MetadataEnrichedRowStreamElement](toInFlightBatch: ToInFlightBatch): ZPipeline[Any, Throwable, Chunk[IncomingElement], OutgoingElement]
