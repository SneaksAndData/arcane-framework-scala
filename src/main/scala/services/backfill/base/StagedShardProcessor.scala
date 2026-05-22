package com.sneaksanddata.arcane.framework
package services.backfill.base

import models.sharding.{BootstrappedShard, StagedShard}
import services.streaming.base.JsonWatermark

import zio.stream.ZPipeline

/** Processes a staged shard
  */
trait StagedShardProcessor:

  type OutgoingElement <: StagedShard
  type IncomingElement <: StagedShard

  /** Processes incoming shard into a different shard, for example Staged -> Completed
    */
  def process: ZPipeline[Any, Throwable, IncomingElement, OutgoingElement]
