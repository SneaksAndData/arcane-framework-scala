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

  /**
   * Prepares shard for staging by creating a table for its data
   * @return
   */
  def prepareShardStage(shard: BootstrappedShard, schema: ArcaneSchema): Task[Unit]

  /**
   * Adds a completed shard to a backfill state
   */
  def addCombinedShard(completionShard: CompletionShard): Task[Unit]

  /**
   * Marks a staged shard table as completed (shard stream exhausted and appended to its staging table)
   */
  def commitStagedShard(shard: StagedShard): Task[StagedShard]

  /**
   * Check if a provided bootstrapped shard has been successfully staged
   */
  def isStaged(shard: BootstrappedShard): Task[Boolean]

  /**
   * Check if a provided staged shard has been successfully added to the combined table
   */
  def isCombined(shard: StagedShard): Task[Option[CompletionShard]]
