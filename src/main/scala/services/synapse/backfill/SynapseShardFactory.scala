package com.sneaksanddata.arcane.framework
package services.synapse.backfill

import models.queries.backfill.synapse.{SynapseLinkShardCommitQuery, SynapseLinkShardStageQuery}
import models.sharding.{BootstrappedShard, CompletionShard, DefaultStagedShard, StagedShard}
import services.backfill.base.ShardFactory

import zio.{ULayer, ZLayer}

/** Backfill shard factory for SynapseLink
  */
final class SynapseShardFactory extends ShardFactory:
  override def createStagedShard(shard: BootstrappedShard): StagedShard = DefaultStagedShard(
    shardSourceEntityName = shard.shardSourceEntityName,
    combinedTableName = shard.combinedTableName,
    targetTableName = shard.targetTableName,
    commitQuery = SynapseLinkShardStageQuery(shard.shardTableName, shard.combinedTableName),
    backfillId = shard.backfillId,
    prefix = shard.prefix
  )

  override def createCompletionShard(shard: StagedShard, watermark: String): CompletionShard = CompletionShard(
    watermark = watermark,
    targetTableName = shard.targetTableName,
    shardSourceEntityName = shard.shardSourceEntityName,
    combinedTableName = shard.combinedTableName,
    commitQuery = SynapseLinkShardCommitQuery(shard.targetTableName, shard.combinedTableName),
    backfillId = shard.backfillId,
    prefix = shard.prefix
  )

object SynapseShardFactory:
  val layer: ULayer[SynapseShardFactory] = ZLayer.succeed(new SynapseShardFactory())
