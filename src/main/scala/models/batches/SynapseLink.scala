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
import models.settings.{TablePropertiesSettings, EmptyTablePropertiesSettings}

object MatchedAppendOnlyDelete:
  def apply(): WhenMatchedDelete = new WhenMatchedDelete {
    override val segmentCondition: Option[String] = Some(
      s"coalesce(${MergeQueryCommons.SOURCE_ALIAS}.IsDelete, false) = true"
    )
  }

object MatchedAppendOnlyUpdate {
  def apply(cols: Seq[String]): WhenMatchedUpdate = new WhenMatchedUpdate {
    override val segmentCondition: Option[String] = Some(
      s"coalesce(${MergeQueryCommons.SOURCE_ALIAS}.IsDelete, false) = false AND ${MergeQueryCommons.SOURCE_ALIAS}.versionnumber > ${MergeQueryCommons.TARGET_ALIAS}.versionnumber"
    )
    override val columns: Seq[String] = cols
  }
}

object NotMatchedAppendOnlyInsert {
  def apply(cols: Seq[String]): WhenNotMatchedInsert = new WhenNotMatchedInsert {
    override val columns: Seq[String] = cols
    override val segmentCondition: Option[String] = Some(
      s"coalesce(${MergeQueryCommons.SOURCE_ALIAS}.IsDelete, false) = false"
    )
  }
}

object SynapseLinkMergeQuery:
  def apply(
      targetName: String,
      sourceQuery: String,
      partitionFields: Seq[String],
      mergeKey: String,
      columns: Seq[String]
  ): MergeQuery =
    MergeQuery(targetName, sourceQuery)
      ++ OnSegment(Map(), mergeKey, partitionFields.filterNot(c => c == mergeKey))
      ++ MatchedAppendOnlyDelete()
      ++ MatchedAppendOnlyUpdate(columns.filterNot(c => c == mergeKey))
      ++ NotMatchedAppendOnlyInsert(columns)

object SynapseLinkBackfillQuery:
  def apply(targetName: String, sourceQuery: String, tablePropertiesSettings: TablePropertiesSettings): OverwriteQuery =
    OverwriteReplaceQuery(sourceQuery, targetName, tablePropertiesSettings)

class SynapseLinkBackfillOverwriteBatch(
    batchName: String,
    batchSchema: ArcaneSchema,
    targetName: String,
    tablePropertiesSettings: TablePropertiesSettings,
    watermarkValue: Option[String]
) extends StagedBackfillOverwriteBatch:

  override val name: String            = batchName
  override val schema: ArcaneSchema    = batchSchema
  override val targetTableName: String = targetName

  override def reduceExpr: String = s"""SELECT * FROM $name""".stripMargin

  override val batchQuery: OverwriteQuery = SynapseLinkBackfillQuery(targetName, reduceExpr, tablePropertiesSettings)

  override val completedWatermarkValue: Option[String] = watermarkValue

object SynapseLinkBackfillOverwriteBatch:
  def apply(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName: String,
      tablePropertiesSettings: TablePropertiesSettings,
      watermarkValue: Option[String]
  ): SynapseLinkBackfillOverwriteBatch =
    new SynapseLinkBackfillOverwriteBatch(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName,
      tablePropertiesSettings,
      watermarkValue
    )

class SynapseLinkMergeBatch(
    batchName: String,
    batchSchema: ArcaneSchema,
    targetName: String,
    tablePropertiesSettings: TablePropertiesSettings,
    mergeKey: String,
    watermarkValue: Option[String]
) extends StagedVersionedBatch
    with MergeableBatch:
  override val name: String            = batchName
  override val schema: ArcaneSchema    = batchSchema
  override val targetTableName: String = targetName

  override def reduceExpr: String =
    // for merge query, we must carry over deletions so they can be applied in a MERGE statement by MatchedAppendOnlyDelete
    s"""SELECT * FROM (
       | SELECT * FROM $name ORDER BY ROW_NUMBER() OVER (PARTITION BY ${schema.mergeKey.name} ORDER BY versionnumber DESC) FETCH FIRST 1 ROWS WITH TIES
       |)""".stripMargin

  override val batchQuery: MergeQuery =
    SynapseLinkMergeQuery(
      targetName = targetName,
      sourceQuery = reduceExpr,
      partitionFields = tablePropertiesSettings.partitionFields,
      mergeKey = mergeKey,
      columns = schema.map(f => f.name)
    )

  override val completedWatermarkValue: Option[String] = watermarkValue

object SynapseLinkMergeBatch:
  def empty(watermarkValue: Option[String]): SynapseLinkMergeBatch =
    new SynapseLinkMergeBatch("", ArcaneSchema.empty(), "", EmptyTablePropertiesSettings, "", watermarkValue)
  def apply(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName: String,
      tablePropertiesSettings: TablePropertiesSettings,
      watermarkValue: Option[String]
  ): SynapseLinkMergeBatch =
    new SynapseLinkMergeBatch(
      batchName,
      batchSchema,
      targetName,
      tablePropertiesSettings,
      batchSchema.mergeKey.name,
      watermarkValue
    )

class SynapseLinkBackfillMergeBatch(
    batchName: String,
    batchSchema: ArcaneSchema,
    targetName: String,
    tablePropertiesSettings: TablePropertiesSettings,
    mergeKey: String,
    watermarkValue: Option[String]
) extends StagedBackfillMergeBatch
    with MergeableBatch:

  override val name: String            = batchName
  override val schema: ArcaneSchema    = batchSchema
  override val targetTableName: String = targetName

  override def reduceExpr: String = s"SELECT * FROM $name"

  override val batchQuery: MergeQuery = SynapseLinkMergeQuery(
    targetName = targetName,
    sourceQuery = reduceExpr,
    partitionFields = tablePropertiesSettings.partitionFields,
    mergeKey = mergeKey,
    columns = schema.map(f => f.name)
  )
  override val completedWatermarkValue: Option[String] = watermarkValue

object SynapseLinkBackfillMergeBatch:
  def apply(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName: String,
      tablePropertiesSettings: TablePropertiesSettings,
      watermarkValue: Option[String]
  ): SynapseLinkBackfillMergeBatch =
    new SynapseLinkBackfillMergeBatch(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName,
      tablePropertiesSettings,
      batchSchema.mergeKey.name,
      watermarkValue
    )
