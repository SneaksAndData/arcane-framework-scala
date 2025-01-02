package com.sneaksanddata.arcane.framework
package services.consumers

import models.querygen.{MergeQuery, OverwriteQuery, StreamingBatchQuery, InitializeQuery}
import models.ArcaneSchema


trait StagedBatch[Query <: StreamingBatchQuery]:
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
  def archiveExpr: String


/**
 * StagedBatch that overwrites the whole table and all partitions that it might have
 */
type StagedInitBatch = StagedBatch[InitializeQuery]

/**
 * StagedBatch that overwrites the whole table and all partitions that it might have
 */
type StagedBackfillBatch = StagedBatch[OverwriteQuery]

/**
 * StagedBatch that updates data in the table
 */
type StagedVersionedBatch = StagedBatch[MergeQuery]
