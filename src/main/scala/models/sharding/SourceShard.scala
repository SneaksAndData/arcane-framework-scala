package com.sneaksanddata.arcane.framework
package models.sharding

import models.queries.StreamingBatchQuery
import models.settings.TableName

/** A shard of data from source to be used by backfills
  */
trait SourceShard:
  val combinedTableName: String
  val targetTableName: TableName
  val shardSourceEntityName: String
  val backfillId: String

  /** Unique shard identifier based on the source entity used to create a shard data stream
    */
  final val shardId =
    s"${shardSourceEntityName.replace("-", "_").replace(".", "_").replace(":", "_").stripSuffix("/").toLowerCase}"

  final val shardTableName: String =
    s"backfill_shard__${backfillId}__$shardId"

case class CompletedShard(
    combinedTableName: String,
    targetTableName: String,
    override val shardSourceEntityName: String,
    override val backfillId: String
) extends StagedShard:
  override val commitQuery: StreamingBatchQuery = new StreamingBatchQuery {
    override def query: String = ""
  }
