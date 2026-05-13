package com.sneaksanddata.arcane.framework
package services.streaming.base

import models.batches.{MergeableBatch, StagedBatch, StagedVersionedBatch}
import models.schemas.{ArcaneSchema, DataRow}
import models.settings.TablePropertiesSettings

import org.apache.iceberg.Table
import zio.Chunk
import zio.stream.ZPipeline

/** A trait that represents a row processor.
  */
trait RowGroupTransformer:

  type OutgoingElement <: StagedBatch
  type IncomingElement = DataRow

  /** Processes the incoming data.
    *
    * @return
    *   ZPipeline (stream source for the stream graph).
    */
  def process(
      schema: ArcaneSchema
  ): ZPipeline[Any, Throwable, IncomingElement, OutgoingElement]
