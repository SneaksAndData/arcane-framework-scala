package com.sneaksanddata.arcane.framework
package services.consumers

import models.querygen.{InitializeQuery, MergeQuery, OverwriteQuery, OverwriteReplaceQuery, StreamingBatchQuery}
import models.ArcaneSchema


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
 * StagedBatch that overwrites the whole table and all partitions that it might have
 */
trait StagedInitBatch extends StagedBatch:
  override type Query = InitializeQuery

/**
 * StagedBatch that performs a backfill operation on the table in CREATE OR REPLACE mode
 */
trait StagedBackfillOverwriteBatch extends StagedBatch:
  override type Query = OverwriteQuery

/**
 * StagedBatch that performs a backfill operation on the table in MERGE mode
 */
trait StagedBackfillMergeBatch extends StagedBatch:
  override type Query = MergeQuery

/**
 * StagedBatch that updates data in the table
 */
trait StagedVersionedBatch extends StagedBatch:
  override type Query = MergeQuery
