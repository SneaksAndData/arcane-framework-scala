package com.sneaksanddata.arcane.framework
package models.sharding

import models.queries.{DefaultShardCommitQuery, StreamingBatchQuery}

/** A staged shard contains a chunk of data from source that has been successfully streamed out
  */
trait StagedShard extends SourceShard:
  val shardSourceEntityName: String
  val commitQuery: StreamingBatchQuery

case class DefaultStagedShard(
                               override val shardId: String,
                               override val shardSourceEntityName: String,
                               override val combinedTableName: String,
                               override val targetTableName: String,
                               override val commitQuery: StreamingBatchQuery
                             ) extends StagedShard
