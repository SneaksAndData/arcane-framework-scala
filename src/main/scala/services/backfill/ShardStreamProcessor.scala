package com.sneaksanddata.arcane.framework
package services.backfill

import models.batches.StagedBatch
import models.schemas.{ArcaneSchema, DataRow}
import models.sharding.{BootstrappedShard, StagedShard}

import zio.stream.ZPipeline

/** Processor for the shard's bound data stream
  */
trait ShardStreamProcessor:

  type OutgoingElement <: StagedShard

  /** Processes the incoming data for the bootstrapped shard into a staged shard.
    */
  def process(
      shard: BootstrappedShard,
      schema: ArcaneSchema
  ): ZPipeline[Any, Throwable, DataRow, OutgoingElement]
