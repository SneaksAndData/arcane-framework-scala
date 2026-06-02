package com.sneaksanddata.arcane.framework
package models.sharding

import models.queries.{DefaultShardCommitQuery, StreamingBatchQuery}

/** A staged shard contains a chunk of data from source that has been successfully streamed out
  */
trait StagedShard extends SourceShard:
  val commitQuery: StreamingBatchQuery

case class DefaultStagedShard(
    override val shardSourceEntityName: String,
    override val combinedTableName: String,
    override val targetTableName: String,
    override val commitQuery: StreamingBatchQuery,
    override val backfillId: String
) extends StagedShard
