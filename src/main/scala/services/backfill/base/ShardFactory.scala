package com.sneaksanddata.arcane.framework
package services.backfill.base

import models.sharding.{BootstrappedShard, CompletionShard, StagedShard}

trait ShardFactory:
  def createStagedShard(shard: BootstrappedShard): StagedShard
  
  def createCompletionShard(shard: StagedShard, watermark: String): CompletionShard
