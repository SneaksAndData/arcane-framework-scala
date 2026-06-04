package com.sneaksanddata.arcane.framework
package models.batches

import models.queries.*
import models.schemas.ArcaneSchema
import models.settings.TableName

trait MergeableBatch:

  /** Name of the target table in the linked Catalog that holds batch data
    */
  val targetTableName: TableName

/** The trait that represents a staged batch of data.
  */
trait StagedBatch:

  type Query <: StreamingBatchQuery

  /** Name of the table in the linked Catalog that holds batch data
    */
  val name: String

  /** Schema of the staging table created fo this batch
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

  /** Check if current batch is an empty batch - batch without name is empty and should be discarded
    * @return
    */
  def isEmpty: Boolean = name.isBlank

/** StagedBatch that updates data in the table
  */
trait StagedVersionedBatch extends StagedBatch:
  override type Query = MergeQuery
