package com.sneaksanddata.arcane.framework
package services.base

import models.schemas.ArcaneSchema

import zio.Task

/** A type class that converts a value to a SQL expression.
  *
  * @tparam A
  *   The type of the value to convert.
  */
trait SqlExpressionConvertable[A]:
  /** Converts a value to a SQL expression.
    *
    * @param a
    *   The value to convert.
    * @return
    *   The SQL expression.
    */
  extension (a: A) def toSqlExpression: String

  /** Gets the name of the value.
    *
    * @param a
    *   The value to convert.
    * @return
    *   The SQL expression.
    */
  extension (a: A) def name: String

/** A type class that converts a value to a SQL expression.
  *
  * @tparam A
  *   The type of the value to convert.
  */
trait ConditionallyApplicable[A]:
  /** Converts a value to a SQL expression.
    *
    * @param a
    *   The value to convert.
    * @return
    *   The SQL expression.
    */
  extension (a: A) def isApplicable: Boolean

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

  /** Migrates the schema of a table.
    *
    * @param newSchema
    *   The new schema coming from the batch.
    * @param tableName
    *   The name of the table.
    * @return
    *   The result of the schema migration operation.
    */
  def migrateSchema(newSchema: ArcaneSchema, tableName: String): Task[Unit]

  /** Cleans up the staging tables in the specific catalog by table name prefix. This method is used to ensure that the
    * staging tables are cleaned up after the streaming job restart.
    *
    * @param stagingCatalogName
    *   The catalog of the staging table.
    * @param stagingSchemaName
    *   The catalog of the staging table.
    * @param tableNamePrefix
    *   The prefix of the staging table name.
    * @return
    *   The list of tables.
    */
  def cleanupStagingTables(stagingCatalogName: String, stagingSchemaName: String, tableNamePrefix: String): Task[Unit]

  /** Creates the target table.
    *
    * @return
    *   The result of the target table creation operation.
    */
  def createTargetTable: Task[Unit]

  /** Creates the backfill staging table.
    *
    * @return
    *   The result of the backfill staging table creation operation.
    */
  def createBackFillTable: Task[Unit]

  /** Runs ANALYZE on the table
    * @return
    */
  def analyzeTable(request: Option[TableAnalyzeRequest]): Task[BatchOptimizationResult]
