package com.sneaksanddata.arcane.framework
package services.backfill.base

import models.backfill.SourceBackfill
import models.schemas.ArcaneSchema
import models.sharding.{BootstrappedShard, CompletionShard, StagedShard}

import zio.Task

/** Processing state for a shard.
  */
enum ShardProcessingState:
  case
    // shard data has been downloaded to the staging warehouse
    STAGED,
    // shard data has been inserted into the combined staging table
    COMBINED

/** Backfill state management service
  */
trait BackfillStateManager:
  final val statePropertyName           = "backfill"
  final val processingStatePropertyName = "processing-state"
  final val watermarkPropertyName       = "shard-watermark"

  type StateImpl <: SourceBackfill

  /** Saves current backfill state to a staging table's metadata
    * @return
    */
  def commitState(state: StateImpl): Task[Unit]

  /** Reads current backfill state from a staging table's metadata
    * @return
    */
  def readState: Task[Option[StateImpl]]

  /** Prepares shard for staging by creating a table for its data
    * @return
    */
  def prepareShardStage(shard: BootstrappedShard, schema: ArcaneSchema): Task[Unit]

  /** Marks a staged shard table as COMBINED
    */
  def commitCombinedShard(completionShard: CompletionShard): Task[Unit]

  /** Marks a staged shard table as STAGED
    */
  def commitStagedShard(shard: StagedShard): Task[Unit]

  /** Check if a provided bootstrapped shard has been successfully staged
    */
  def isStaged(shard: BootstrappedShard): Task[Boolean]

  /** Check if a provided staged shard has been successfully added to the combined table
    */
  def isCombined(shard: StagedShard): Task[Option[CompletionShard]]
