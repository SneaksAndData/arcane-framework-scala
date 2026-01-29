package com.sneaksanddata.arcane.framework
package models.batches

import models.queries.*
import models.schemas.ArcaneSchema

trait MergeableBatch:

  /** Name of the target table in the linked Catalog that holds batch data
    */
  val targetTableName: String

/** The trait that represents a staged batch of data.
  */
trait StagedBatch:

  type Query <: StreamingBatchQuery

  /** Name of the table in the linked Catalog that holds batch data
    */
  val name: String

  /** Schema for the table that holds batch data
    */
  val schema: ArcaneSchema

  /** Query to be used to process this batch
    */
  val batchQuery: Query

  /** Serialized watermark value that is supplied if the batch is completed
    */
  val completedWatermarkValue: Option[String]

  /** Query that aggregates transactions in the batch to enable merge or overwrite
    * @return
    *   SQL query text
    */
  def reduceExpr: String

  /** Query that should be used to dispose of this batch data.
    * @return
    *   SQL query text
    */
  def disposeExpr: String = s"DROP TABLE $name"

  /** Check if current batch is an empty batch - batch without schema or name is empty and should be discarded
    * @return
    */
  def isEmpty: Boolean = name.isBlank || schema.isEmpty

/** Common trait for StagedBatch that performs a backfill operation on the table.
  */
trait StagedBackfillBatch extends StagedBatch with MergeableBatch

/** StagedBatch that performs a backfill operation on the table in CREATE OR REPLACE mode.
  */
trait StagedBackfillOverwriteBatch extends StagedBackfillBatch:
  override type Query = OverwriteQuery

/** StagedBatch that performs a backfill operation on the table in MERGE mode.
  */
trait StagedBackfillMergeBatch extends StagedBackfillBatch:
  override type Query = MergeQuery

/** StagedBatch that updates data in the table
  */
trait StagedVersionedBatch extends StagedBatch:
  override type Query = MergeQuery
