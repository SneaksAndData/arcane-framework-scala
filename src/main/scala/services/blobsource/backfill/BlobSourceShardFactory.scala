package com.sneaksanddata.arcane.framework
package services.blobsource.backfill

import models.queries.backfill.blob.{BlobShardCommitQuery, BlobShardStageQuery}
import models.sharding.{BootstrappedShard, CompletionShard, DefaultStagedShard, StagedShard}
import services.backfill.base.ShardFactory
import services.naming.NameGenerator

import com.sneaksanddata.arcane.framework.models.batches.UpsertBlobMergeQuery
import zio.{Task, ZIO, ZLayer}

class BlobSourceShardFactory(nameGenerator: NameGenerator) extends ShardFactory:
  /** Staged shard provisioner. Commit query targets combine table.
    */
  override def createStagedShard(shard: BootstrappedShard): Task[StagedShard] =
    for shardTableName <- nameGenerator.getShardTableName(shard)
    yield DefaultStagedShard(
      shardSourceEntityName = shard.shardSourceEntityName,
      combinedTableName = shard.combinedTableName,
      targetTableName = shard.targetTableName,
      commitQuery = BlobShardStageQuery(shardTableName, shard.combinedTableName),
      resetQuery = UpsertBlobMergeQuery(
        targetName = shard.combinedTableName,
        sourceQuery = s"SELECT * FROM $shardTableName",
        partitionFields = Seq(),
        mergeKey = shard.shardStream._2.mergeKey.name,
        columns = shard.shardStream._2.map(f => f.name)
      ),
      backfillId = shard.backfillId
    )

  /** Completion shard provisioner. Commit query swaps data from combine into a target table.
    */
  override def createCompletionShard(shard: StagedShard, watermark: String): Task[CompletionShard] = ZIO.succeed(
    CompletionShard(
      watermark = watermark,
      targetTableName = shard.targetTableName,
      shardSourceEntityName = shard.shardSourceEntityName,
      combinedTableName = shard.combinedTableName,
      commitQuery = BlobShardCommitQuery(shard.targetTableName, shard.combinedTableName),
      backfillId = shard.backfillId
    )
  )

object BlobSourceShardFactory:
  val layer = ZLayer {
    for nameGenerator <- ZIO.service[NameGenerator]
    yield new BlobSourceShardFactory(nameGenerator)
  }
