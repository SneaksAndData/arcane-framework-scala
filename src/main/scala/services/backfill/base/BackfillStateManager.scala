package com.sneaksanddata.arcane.framework
package services.backfill.base

import models.backfill.SourceBackfill

import com.sneaksanddata.arcane.framework.models.schemas.ArcaneSchema
import com.sneaksanddata.arcane.framework.models.sharding.{BootstrappedShard, CompletionShard, StagedShard}
import upickle.ReadWriter
import zio.Task

/**
 * Backfill state management service
 */
trait BackfillStateManager:
  final val statePropertyName = "backfill"
  final val stagedShardPropertyName = "staged"
  
  type StateImpl <: SourceBackfill

  /**
   * Saves current backfill state to a staging table's metadata
   * @return
   */
  def commitState(state: StateImpl)(implicit rw: ReadWriter[StateImpl]): Task[Unit]

  /**
   * Reads current backfill state from a staging table's metadata
   * @param rw
   * @return
   */
  def readState(implicit rw: ReadWriter[StateImpl]): Task[Option[StateImpl]] 
  def prepareShardCommit(shard: BootstrappedShard, schema: ArcaneSchema): Task[String]
  def addCombinedShard(completionShard: CompletionShard): Task[Unit]
  def commitStagedShard(shard: StagedShard): Task[StagedShard]
  
