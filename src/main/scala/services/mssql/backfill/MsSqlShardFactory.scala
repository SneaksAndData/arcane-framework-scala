package com.sneaksanddata.arcane.framework
package services.mssql.backfill

import models.queries.backfill.mssql.{MsSqlShardCommitQuery, MsSqlShardStageQuery}
import models.sharding.{BootstrappedShard, CompletionShard, DefaultStagedShard, StagedShard}
import services.backfill.base.ShardFactory
import services.naming.NameGenerator

import zio.{Task, ZIO, ZLayer}

/** Backfill shard factory for Sql Server source
  */
class MsSqlShardFactory(nameGenerator: NameGenerator) extends ShardFactory:
  /** Staged shard provisioner. Commit query targets combine table.
    */
  override def createStagedShard(shard: BootstrappedShard): Task[StagedShard] = for
    shardTableName <- nameGenerator.getShardTableName(shard)
  yield DefaultStagedShard(
    shardSourceEntityName = shard.shardSourceEntityName,
    combinedTableName = shard.combinedTableName,
    targetTableName = shard.targetTableName,
    commitQuery = MsSqlShardStageQuery(shardTableName, shard.combinedTableName),
    backfillId = shard.backfillId
  )  

  /** Completion shard provisioner. Commit query swaps data from combine into a target table.
    */
  override def createCompletionShard(shard: StagedShard, watermark: String): Task[CompletionShard] = ZIO.succeed(CompletionShard(
    watermark = watermark,
    targetTableName = shard.targetTableName,
    shardSourceEntityName = shard.shardSourceEntityName,
    combinedTableName = shard.combinedTableName,
    commitQuery = MsSqlShardCommitQuery(shard.targetTableName, shard.combinedTableName),
    backfillId = shard.backfillId
  ))

object MsSqlShardFactory:
  val layer = ZLayer {
    for
      nameGenerator <- ZIO.service[NameGenerator]
    yield new MsSqlShardFactory(nameGenerator) 
  }
