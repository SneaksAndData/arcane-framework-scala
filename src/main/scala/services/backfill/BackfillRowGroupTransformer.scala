package com.sneaksanddata.arcane.framework
package services.backfill

import models.batches.StagedBatch
import models.schemas.{ArcaneSchema, DataRow}

import com.sneaksanddata.arcane.framework.models.sharding.SourceShard
import zio.stream.ZPipeline

trait BackfillRowGroupTransformer:

  type OutgoingElement <: StagedBatch
  type IncomingElement = DataRow

  /** Processes the incoming data.
   *
   * @return
   *   ZPipeline (stream source for the stream graph).
   */
  def process(
                shard: SourceShard,
               schema: ArcaneSchema
             ): ZPipeline[Any, Throwable, IncomingElement, OutgoingElement]

