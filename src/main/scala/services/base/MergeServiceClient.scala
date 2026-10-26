package com.sneaksanddata.arcane.framework
package services.base

import models.batches.StagedBatch
import models.sharding.StagedShard

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

  def mergeShard(shard: StagedShard): Task[ShardResetResult]

  /** Executes an arbitrary SQL query on the merge client.
    *
    * @param query
    *   The SQL query to execute.
    * @param queryName
    *   The name/identifier of the query.
    * @param operation
    *   The operation name.
    */
  def execute(query: String, queryName: String, operation: String): Task[Unit]
