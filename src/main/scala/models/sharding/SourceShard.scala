package com.sneaksanddata.arcane.framework
package models.sharding

import services.streaming.base.{JsonWatermark, SourceWatermark, StructuredZStream}

import zio.stream.ZStream

/**
 * A shard of data from source to be used by backfills
 */
trait SourceShard:
  val shardId: String

/**
 * A staged shard is a child source for the graph running backfill of the parent source
 */
trait StagedShard extends SourceShard:
  val shardStream: StructuredZStream

/**
 * A completed shard is a shard that has finished streaming and is ready for cleanup
  */
trait CompletedShard extends SourceShard

/**
 * A shard containing a watermark used to sign off the backfill
 */
trait WatermarkShard[WatermarkType <: SourceWatermark[String] & JsonWatermark] extends SourceShard:
  val watermark: WatermarkType


case class JsonWatermarkShard(watermark: SourceWatermark[String] & JsonWatermark) extends WatermarkShard[SourceWatermark[String] & JsonWatermark]:
  override val shardId: String = "watermark"

object SourceShardExtensions:
  extension (shard: SourceShard)
    def getStagingTableName(prefix: String): String = s"${prefix}_${shard.shardId}"