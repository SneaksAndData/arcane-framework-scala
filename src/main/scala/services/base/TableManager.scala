package com.sneaksanddata.arcane.framework
package services.base

import zio.Task

/**
 * A type class that converts a value to a SQL expression.
 *
 * @tparam A The type of the value to convert.
 */
trait SqlExpressionConvertable[A]:
  /**
   * Converts a value to a SQL expression.
   *
   * @param a The value to convert.
   * @return The SQL expression.
   */
  extension (a: A) def toSqlExpression: String

  /**
   * Gets the name of the value.
   *
   * @param a The value to convert.
   * @return The SQL expression.
   */
  extension (a: A) def name: String

/**
 * A type class that converts a value to a SQL expression.
 *
 * @tparam A The type of the value to convert.
 */
trait ConditionallyApplicable[A]:
  /**
   * Converts a value to a SQL expression.
   *
   * @param a The value to convert.
   * @return The SQL expression.
   */
  extension (a: A) def isApplicable: Boolean

/**
 * The result of a table optimization operation.
 */
case class BatchOptimizationResult(skipped: Boolean)

object BatchOptimizationResult:
  /**
   * Creates a new instance of the result.
   *
   * @return The result.
   */
  def apply(skipped: Boolean): BatchOptimizationResult = new BatchOptimizationResult(skipped)

/**
 * A service that is responsible for managing tables.
 */
trait TableManager:
  
  type TableOptimizationRequest
  
  type SnapshotExpirationRequest
  
  type OrphanFilesExpirationRequest

  /**
   * Optimizes a table.
   *
   * @param batchOptimizationRequest The optimization request.
   * @return The result of the optimization operation.
   */
  def optimizeTable(batchOptimizationRequest: TableOptimizationRequest): Task[BatchOptimizationResult]

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

