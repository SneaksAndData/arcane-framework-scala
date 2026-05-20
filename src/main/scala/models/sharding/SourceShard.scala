package com.sneaksanddata.arcane.framework
package models.sharding

import models.queries.{OverwriteReplaceQuery, ShardCommitQuery, StreamingBatchQuery}
import services.streaming.base.{JsonWatermark, SourceWatermark, StructuredZStream}

import com.sneaksanddata.arcane.framework.models.settings.EmptyTablePropertiesSettings
import upickle.ReadWriter
import zio.stream.ZStream

/** A shard of data from source to be used by backfills
  */
trait SourceShard:
  val shardId: String
  val combinedTableName: String
  val targetTableName: String

  val shardTableName: String =
    s"${sys.env.getOrElse("STREAMCONTEXT__STREAM_ID", "undefined").toLowerCase}_${shardId.replace("-", "_")}"

case class CompletedShard(
    shardId: String,
    combinedTableName: String,
    targetTableName: String,
    override val shardSourceEntityName: String
) extends StagedShard
