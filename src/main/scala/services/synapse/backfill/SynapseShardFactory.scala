package com.sneaksanddata.arcane.framework
package services.synapse.backfill

import models.batches.SynapseLinkMergeQuery
import models.queries.backfill.synapse.{SynapseLinkShardCommitQuery, SynapseLinkShardStageQuery}
import models.sharding.{BootstrappedShard, CompletionShard, DefaultStagedShard, StagedShard}
import services.backfill.base.ShardFactory
import services.naming.NameGenerator

import zio.{Task, ZIO, ZLayer}

/** Backfill shard factory for SynapseLink
  */
final class SynapseShardFactory(nameGenerator: NameGenerator) extends ShardFactory:
  override def createStagedShard(shard: BootstrappedShard): Task[StagedShard] =
    for shardTableName <- nameGenerator.getShardTableName(shard)
    yield DefaultStagedShard(
      shardSourceEntityName = shard.shardSourceEntityName,
      combinedTableName = shard.combinedTableName,
      targetTableName = shard.targetTableName,
      commitQuery = SynapseLinkShardStageQuery(shardTableName, shard.combinedTableName),
      mergeQuery = SynapseLinkMergeQuery(
        targetName = shard.combinedTableName,
        sourceQuery = s"SELECT * FROM $shardTableName",
        partitionFields = Seq(),
        mergeKey = shard.shardStream._2.mergeKey.name,
        columns = shard.shardStream._2.map(f => f.name)
      ),
      backfillId = shard.backfillId
    )

  override def createCompletionShard(shard: StagedShard, watermark: String): Task[CompletionShard] = ZIO.succeed(
    CompletionShard(
      watermark = watermark,
      targetTableName = shard.targetTableName,
      shardSourceEntityName = shard.shardSourceEntityName,
      combinedTableName = shard.combinedTableName,
      commitQuery = SynapseLinkShardCommitQuery(shard.targetTableName, shard.combinedTableName),
      backfillId = shard.backfillId
    )
  )

object SynapseShardFactory:
  val layer = ZLayer {
    for nameGenerator <- ZIO.service[NameGenerator]
    yield new SynapseShardFactory(nameGenerator)
  }
