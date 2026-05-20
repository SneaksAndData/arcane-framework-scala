package com.sneaksanddata.arcane.framework
package services.backfill.base

import models.sharding.{BootstrappedShard, CompletionShard, StagedShard}

/**
 * Source-specific shard builder. Since shard commit queries are source-dependent, each source must implement their own factory.
 */
trait ShardFactory:
  /**
   * Staged shard provisioner. Commit query targets combine table.
   */
  def createStagedShard(shard: BootstrappedShard): StagedShard

  /**
   * Completion shard provisioner. Commit query swaps data from combine into a target table.
   */
  def createCompletionShard(shard: StagedShard, watermark: String): CompletionShard
