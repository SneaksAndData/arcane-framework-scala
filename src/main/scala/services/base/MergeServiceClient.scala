package com.sneaksanddata.arcane.framework
package services.base

import models.batches.StagedBatch

import com.sneaksanddata.arcane.framework.models.sharding.StagedShard
import zio.Task

/** The result of applying a batch.
  */
type BatchApplicationResult = Boolean

/** Result of the shard commit into the combine table
  */
type ShardCommitResult = Boolean

/** Result of the shard reset (removing staged shard from the combined table)
 */
type ShardResetResult = Boolean

/** A service client that merges data batches.
  */
trait MergeServiceClient:

  /** Applies a batch to the target table.
    *
    * @param batch
    *   The batch to apply.
    * @return
    *   The result of applying the batch.
    */
  def applyBatch(batch: StagedBatch): Task[BatchApplicationResult]

  /** Commits a shard to the combine table
    * @return
    */
  def commitShard(shard: StagedShard): Task[ShardCommitResult]
  
  def resetShard(shard: StagedShard): Task[ShardResetResult]
