package com.sneaksanddata.arcane.framework
package models.sharding

import models.queries.{OverwriteReplaceQuery, DefaultShardCommitQuery, StreamingBatchQuery}
import services.streaming.base.{JsonWatermark, SourceWatermark, StructuredZStream}

import com.sneaksanddata.arcane.framework.models.settings.EmptyTablePropertiesSettings
import upickle.ReadWriter
import zio.stream.ZStream

/** A shard of data from source to be used by backfills
  */
trait SourceShard:
  val combinedTableName: String
  val targetTableName: String
  val shardSourceEntityName: String
  val streamId: String
  val backfillId: String

  /**
   * Unique shard identifier based on the source entity used to create a shard data stream
   */
  final val shardId = s"${shardSourceEntityName.replace("-", "_").replace(".", "_").replace(":", "_")}"
  
  final val shardTableName: String =
    s"backfill_shard__${streamId}__${backfillId}__$shardId"

case class CompletedShard(
    combinedTableName: String,
    targetTableName: String,
    override val shardSourceEntityName: String,
    override val streamId: String,
    override val backfillId: String
) extends StagedShard:
  override val commitQuery: StreamingBatchQuery = new StreamingBatchQuery {
    override def query: String = ""
  }
