package com.sneaksanddata.arcane.framework
package models.sharding

import models.queries.{ShardCommitQuery, StreamingBatchQuery}

/** A staged shard contains a chunk of data from source that has been successfully streamed out
  */
trait StagedShard extends SourceShard:
  val shardSourceEntityName: String
  val commitQuery: StreamingBatchQuery = ShardCommitQuery(combinedTableName, shardTableName)

object StagedShard:
  def apply(
      id: String,
      shardSourceName: String,
      combinedTableName: String,
      targetTableName: String
  ): StagedShard = new StagedShard {
    override val shardSourceEntityName: String = shardSourceName
    override val shardId: String               = id
    override val combinedTableName: String     = combinedTableName
    override val targetTableName: String       = targetTableName
  }

  extension (shard: BootstrappedShard)
    def toStaged: StagedShard = StagedShard(
      shard.shardId,
      shard.shardSourceEntityName,
      shard.combinedTableName,
      shard.targetTableName
    )
