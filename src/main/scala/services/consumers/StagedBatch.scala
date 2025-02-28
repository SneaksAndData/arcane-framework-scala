package com.sneaksanddata.arcane.framework
package services.consumers

import models.querygen.{InitializeQuery, MergeQuery, OverwriteQuery, OverwriteReplaceQuery, StreamingBatchQuery}
import models.ArcaneSchema

trait MergeableBatch:

  /**
   * Name of the target table in the linked Catalog that holds batch data
   */
  val targetTableName: String

trait ArchivableeBatch:

  /**
   * Name of the target table in the linked Catalog that holds batch data
   */
  val archiveTableName: String


trait StagedBatch:

  type Query <: StreamingBatchQuery

  /**
   * Name of the table in the linked Catalog that holds batch data
   */
  val name: String
  /**
   * Schema for the table that holds batch data
   */
  val schema: ArcaneSchema
  /**
   * Query to be used to process this batch
   */
  val batchQuery: Query

  /**
   * Query that aggregates transactions in the batch to enable merge or overwrite
   * @return SQL query text
   */
  def reduceExpr: String

  /**
   * Query that should be used to archive this batch data
   * @return SQL query text
   */
  def archiveExpr(archiveTableName: String): String

  /**
   * Query that should be used to archive this batch data
   * @return SQL query text
   */
  def archiveExpr(arcaneSchema: ArcaneSchema): String

  /**
   * Query that should be used to dispose of this batch data.
   * @return SQL query text
   */
  def disposeExpr: String = s"DROP TABLE $name"

/**
 * StagedBatch initializes the table.
 */
trait StagedInitBatch extends StagedBatch:
  override type Query = InitializeQuery

/**
 * Common trait for StagedBatch that performs a backfill operation on the table.
 */
trait StagedBackfillBatch extends StagedBatch

/**
 * StagedBatch that performs a backfill operation on the table in CREATE OR REPLACE mode.
 */
trait StagedBackfillOverwriteBatch extends StagedBackfillBatch:
  override type Query = OverwriteQuery

/**
 * StagedBatch that performs a backfill operation on the table in MERGE mode.
 */
trait StagedBackfillMergeBatch extends StagedBackfillBatch:
  override type Query = MergeQuery

/**
 * StagedBatch that updates data in the table
 */
trait StagedVersionedBatch extends StagedBatch:
  override type Query = MergeQuery
