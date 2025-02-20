package com.sneaksanddata.arcane.framework
package services.base

import zio.Task

/**
 * The result of a table optimization operation.
 */
class BatchOptimizationResult

/**
 * A service that is responsible for managing tables.
 */
trait TableManager:
  
  type BatchOptimizationRequest
  
  type SnapshotExpirationRequest
  
  type OrphanFilesExpirationRequest

  /**
   * Optimizes a table.
   *
   * @param batchOptimizationRequest The optimization request.
   * @return The result of the optimization operation.
   */
  def optimizeTable(batchOptimizationRequest: BatchOptimizationRequest): Task[BatchOptimizationResult]

  /**
   * Expires snapshots.
   *
   * @param snapshotExpirationRequest The snapshot expiration request.
   * @return The result of the snapshot expiration operation.
   */
  def expireSnapshots(snapshotExpirationRequest: SnapshotExpirationRequest): Task[BatchOptimizationResult]

  /**
   * Expires orphan files.
   *
   * @param orphanFilesExpirationRequest The orphan files expiration request.
   * @return The result of the orphan files expiration operation.
   */
  def expireOrphanFiles(orphanFilesExpirationRequest: OrphanFilesExpirationRequest): Task[BatchOptimizationResult]

