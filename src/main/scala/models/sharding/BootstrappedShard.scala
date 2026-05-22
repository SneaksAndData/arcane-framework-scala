package com.sneaksanddata.arcane.framework
package models.sharding

import models.schemas.{ArcaneSchema, DataRow}
import services.streaming.base.StructuredZStream

import zio.stream.ZStream

/** A shard of source data that has been successfully bootstrapped and is ready for staging
  */
trait BootstrappedShard extends SourceShard:
  val shardStream: StructuredZStream
  val shardSourceEntityName: String

case class DefaultBootstrappedShard(
    override val shardStream: (ZStream[Any, Throwable, DataRow], ArcaneSchema),
    override val shardSourceEntityName: String,
    override val combinedTableName: String,
    override val targetTableName: String,
    override val backfillId: String
) extends BootstrappedShard
