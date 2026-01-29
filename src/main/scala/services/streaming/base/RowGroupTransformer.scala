package com.sneaksanddata.arcane.framework
package services.streaming.base

import models.batches.{MergeableBatch, StagedVersionedBatch}
import models.schemas.{ArcaneSchema, DataRow}
import models.settings.TablePropertiesSettings
import services.streaming.processors.transformers.IndexedStagedBatches

import org.apache.iceberg.Table
import zio.Chunk
import zio.stream.ZPipeline

trait ToInFlightBatch[T]:
  /** Converts the staged batches to the outgoing type.
    *
    * @param batches
    *   The staged batches.
    * @param batchIndex
    *   The batch index.
    * @param others
    *   The other elements.
    * @return
    *   The outgoing type.
    */
  extension (batches: Iterable[StagedVersionedBatch])
    def toBatch[Element: MetadataEnrichedRowStreamElement](batchIndex: Long, others: Chunk[Element]): T

/** A trait that represents a row processor.
  */
trait RowGroupTransformer:

  type OutgoingElement <: IndexedStagedBatches

  type OnStagingTablesComplete = (Iterable[StagedVersionedBatch & MergeableBatch], Long, Chunk[Any]) => OutgoingElement
  type OnBatchStaged =
    (
        Option[Table],
        String,
        String,
        ArcaneSchema,
        String,
        TablePropertiesSettings,
        Option[String]
    ) => StagedVersionedBatch & MergeableBatch

  type IncomingElement = DataRow

  /** Processes the incoming data.
    *
    * @return
    *   ZPipeline (stream source for the stream graph).
    */
  def process(
      onStagingTablesComplete: OnStagingTablesComplete,
      onBatchStaged: OnBatchStaged
  ): ZPipeline[Any, Throwable, Chunk[IncomingElement], OutgoingElement]
