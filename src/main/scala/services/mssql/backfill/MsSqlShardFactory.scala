package com.sneaksanddata.arcane.framework
package services.mssql.backfill

import models.queries.backfill.mssql.{MsSqlShardCommitQuery, MsSqlShardStageQuery}
import models.sharding.{BootstrappedShard, CompletionShard, DefaultStagedShard, StagedShard}
import services.backfill.base.ShardFactory

import zio.{ULayer, ZLayer}

/**
 * Backfill shard factory for Sql Server source
 */
class MsSqlShardFactory extends ShardFactory:
  /** Staged shard provisioner. Commit query targets combine table.
   */
  override def createStagedShard(shard: BootstrappedShard): StagedShard = DefaultStagedShard(
    shardSourceEntityName = shard.shardSourceEntityName,
    combinedTableName = shard.combinedTableName,
    targetTableName = shard.targetTableName,
    commitQuery = MsSqlShardStageQuery(shard.shardTableName, shard.combinedTableName),
    backfillId = shard.backfillId
  )

  /** Completion shard provisioner. Commit query swaps data from combine into a target table.
   */
  override def createCompletionShard(shard: StagedShard, watermark: String): CompletionShard = CompletionShard(
    watermark = watermark,
    targetTableName = shard.targetTableName,
    shardSourceEntityName = shard.shardSourceEntityName,
    combinedTableName = shard.combinedTableName,
    commitQuery = MsSqlShardCommitQuery(shard.targetTableName, shard.combinedTableName),
    backfillId = shard.backfillId
  )

object MsSqlShardFactory:
  val layer: ULayer[MsSqlShardFactory] = ZLayer.succeed(new MsSqlShardFactory())
