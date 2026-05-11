package com.sneaksanddata.arcane.framework
package services.base

import models.schemas.ArcaneSchema

import zio.Task


/** The result of a table optimization operation.
  */
case class BatchOptimizationResult(skipped: Boolean)

object BatchOptimizationResult:
  /** Creates a new instance of the result.
    *
    * @return
    *   The result.
    */
  def apply(skipped: Boolean): BatchOptimizationResult = new BatchOptimizationResult(skipped)

/** A service that is responsible for managing tables.
  */
trait TableManager:

  type TableOptimizationRequest

  type SnapshotExpirationRequest

  type OrphanFilesExpirationRequest

  type TableAnalyzeRequest

  /** Optimizes a table.
    *
    * @param batchOptimizationRequest
    *   The optimization request.
    * @return
    *   The result of the optimization operation.
    */
  def optimizeTable(batchOptimizationRequest: Option[TableOptimizationRequest]): Task[BatchOptimizationResult]

  /** Expires snapshots.
    *
    * @param snapshotExpirationRequest
    *   The snapshot expiration request.
    * @return
    *   The result of the snapshot expiration operation.
    */
  def expireSnapshots(snapshotExpirationRequest: Option[SnapshotExpirationRequest]): Task[BatchOptimizationResult]

  /** Expires orphan files.
    *
    * @param orphanFilesExpirationRequest
    *   The orphan files expiration request.
    * @return
    *   The result of the orphan files expiration operation.
    */
  def expireOrphanFiles(
      orphanFilesExpirationRequest: Option[OrphanFilesExpirationRequest]
  ): Task[BatchOptimizationResult]

  /** Runs ANALYZE on the table
    * @return
    */
  def analyzeTable(request: Option[TableAnalyzeRequest]): Task[BatchOptimizationResult]
