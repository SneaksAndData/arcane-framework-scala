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
    tablePropertiesSettings: TablePropertiesSettings
) extends StagedBackfillOverwriteBatch:

  override val name: String            = batchName
  override val schema: ArcaneSchema    = batchSchema
  override val targetTableName: String = targetName

  override def reduceExpr: String = s"""SELECT * FROM $name""".stripMargin

  override val batchQuery: OverwriteQuery = SynapseLinkBackfillQuery(targetName, reduceExpr, tablePropertiesSettings)

object SynapseLinkBackfillOverwriteBatch:
  def apply(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName: String,
      tablePropertiesSettings: TablePropertiesSettings
  ): SynapseLinkBackfillOverwriteBatch =
    new SynapseLinkBackfillOverwriteBatch(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName,
      tablePropertiesSettings
    )

class SynapseLinkMergeBatch(
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

object SynapseLinkMergeBatch:
  def apply(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName: String,
      tablePropertiesSettings: TablePropertiesSettings
  ): SynapseLinkMergeBatch =
    new SynapseLinkMergeBatch(batchName, batchSchema, targetName, tablePropertiesSettings, batchSchema.mergeKey.name)

class SynapseLinkBackfillMergeBatch(
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

  override val batchQuery: MergeQuery = SynapseLinkMergeQuery(
    targetName = targetName,
    sourceQuery = reduceExpr,
    partitionFields = tablePropertiesSettings.partitionFields,
    mergeKey = mergeKey,
    columns = schema.map(f => f.name)
  )

object SynapseLinkBackfillMergeBatch:
  def apply(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName: String,
      tablePropertiesSettings: TablePropertiesSettings
  ): SynapseLinkBackfillMergeBatch =
    new SynapseLinkBackfillMergeBatch(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName,
      tablePropertiesSettings,
      batchSchema.mergeKey.name
    )
