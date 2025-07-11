package com.sneaksanddata.arcane.framework
package models.batches

import models.queries.{
  MergeQuery,
  MergeQueryCommons,
  OnSegment,
  OverwriteQuery,
  OverwriteReplaceQuery,
  WhenMatchedDelete,
  WhenMatchedUpdate,
  WhenNotMatchedInsert
}
import models.schemas.ArcaneSchema
import models.settings.TablePropertiesSettings

object MatchedUpdate {
  def apply(cols: Seq[String]): WhenMatchedUpdate = new WhenMatchedUpdate {
    override val segmentCondition: Option[String] = None
    override val columns: Seq[String]             = cols
  }
}

object NotMatchedInsert {
  def apply(cols: Seq[String]): WhenNotMatchedInsert = new WhenNotMatchedInsert {
    override val columns: Seq[String]             = cols
    override val segmentCondition: Option[String] = None
  }
}

object UpsertBlobMergeQuery:
  def apply(
      targetName: String,
      sourceQuery: String,
      partitionFields: Seq[String],
      mergeKey: String,
      columns: Seq[String]
  ): MergeQuery =
    MergeQuery(targetName, sourceQuery)
      ++ OnSegment(Map(), mergeKey, partitionFields.filterNot(c => c == mergeKey))
      ++ MatchedUpdate(columns)
      ++ NotMatchedInsert(columns.filterNot(c => c == mergeKey))

object UpsertBlobBackfillQuery:
  def apply(targetName: String, sourceQuery: String, tablePropertiesSettings: TablePropertiesSettings): OverwriteQuery =
    OverwriteReplaceQuery(sourceQuery, targetName, tablePropertiesSettings)

class UpsertBlobBackfillOverwriteBatch(
    batchName: String,
    batchSchema: ArcaneSchema,
    targetName: String,
    tablePropertiesSettings: TablePropertiesSettings
) extends StagedBackfillOverwriteBatch:

  override val name: String            = batchName
  override val schema: ArcaneSchema    = batchSchema
  override val targetTableName: String = targetName

  override def reduceExpr: String = s"""SELECT * FROM $name""".stripMargin

  override val batchQuery: OverwriteQuery = UpsertBlobBackfillQuery(targetName, reduceExpr, tablePropertiesSettings)

object UpsertBlobBackfillOverwriteBatch:
  def apply(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName: String,
      tablePropertiesSettings: TablePropertiesSettings
  ): UpsertBlobBackfillOverwriteBatch =
    new UpsertBlobBackfillOverwriteBatch(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName,
      tablePropertiesSettings
    )

class UpsertBlobMergeBatch(
    batchName: String,
    batchSchema: ArcaneSchema,
    targetName: String,
    tablePropertiesSettings: TablePropertiesSettings,
    mergeKey: String
) extends StagedVersionedBatch
    with MergeableBatch:
  override val name: String            = batchName
  override val schema: ArcaneSchema    = batchSchema
  override val targetTableName: String = targetName

  override def reduceExpr: String =
    // for merge query, we must carry over deletions so they can be applied in a MERGE statement by MatchedAppendOnlyDelete
    s"""SELECT * FROM (
       | SELECT * FROM $name ORDER BY ROW_NUMBER() OVER (PARTITION BY ${schema.mergeKey.name} ORDER BY createdon DESC) FETCH FIRST 1 ROWS WITH TIES
       |)""".stripMargin

  override val batchQuery: MergeQuery =
    UpsertBlobMergeQuery(
      targetName = targetName,
      sourceQuery = reduceExpr,
      partitionFields = tablePropertiesSettings.partitionFields,
      mergeKey = mergeKey,
      columns = schema.map(f => f.name)
    )

object UpsertBlobMergeBatch:
  def apply(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName: String,
      tablePropertiesSettings: TablePropertiesSettings
  ): UpsertBlobMergeBatch =
    new UpsertBlobMergeBatch(batchName, batchSchema, targetName, tablePropertiesSettings, batchSchema.mergeKey.name)

class UpsertBlobBackfillMergeBatch(
    batchName: String,
    batchSchema: ArcaneSchema,
    targetName: String,
    tablePropertiesSettings: TablePropertiesSettings,
    mergeKey: String
) extends StagedBackfillMergeBatch
    with MergeableBatch:

  override val name: String            = batchName
  override val schema: ArcaneSchema    = batchSchema
  override val targetTableName: String = targetName

  override def reduceExpr: String = s"SELECT * FROM $name"

  override val batchQuery: MergeQuery = UpsertBlobMergeQuery(
    targetName = targetName,
    sourceQuery = reduceExpr,
    partitionFields = tablePropertiesSettings.partitionFields,
    mergeKey = mergeKey,
    columns = schema.map(f => f.name)
  )

object UpsertBlobBackfillMergeBatch:
  def apply(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName: String,
      tablePropertiesSettings: TablePropertiesSettings
  ): UpsertBlobBackfillMergeBatch =
    new UpsertBlobBackfillMergeBatch(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName,
      tablePropertiesSettings,
      batchSchema.mergeKey.name
    )
