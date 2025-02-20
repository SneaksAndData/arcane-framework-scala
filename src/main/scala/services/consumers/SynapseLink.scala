package com.sneaksanddata.arcane.framework
package services.consumers

import models.ArcaneSchema
import models.querygen.{MergeQuery, MergeQueryCommons, OnSegment, OverwriteQuery, OverwriteReplaceQuery, WhenMatchedDelete, WhenMatchedUpdate, WhenNotMatchedInsert}
import models.settings.TablePropertiesSettings

object MatchedAppendOnlyDelete:
  def apply(): WhenMatchedDelete = new WhenMatchedDelete {
    override val segmentCondition: Option[String] = Some(s"coalesce(${MergeQueryCommons.SOURCE_ALIAS}.IsDelete, false) = true")
  }

object MatchedAppendOnlyUpdate {
  def apply(cols: Seq[String]): WhenMatchedUpdate = new WhenMatchedUpdate {
    override val segmentCondition: Option[String] = Some(s"coalesce(${MergeQueryCommons.SOURCE_ALIAS}.IsDelete, false) = false AND ${MergeQueryCommons.SOURCE_ALIAS}.versionnumber > ${MergeQueryCommons.TARGET_ALIAS}.versionnumber")
    override val columns: Seq[String] = cols
  }
}

object NotMatchedAppendOnlyInsert {
  def apply(cols: Seq[String]): WhenNotMatchedInsert = new WhenNotMatchedInsert {
    override val columns: Seq[String] = cols
    override val segmentCondition: Option[String] = Some(s"coalesce(${MergeQueryCommons.SOURCE_ALIAS}.IsDelete, false) = false")
  }
}

object SynapseLinkMergeQuery:
  def apply(targetName: String, sourceQuery: String, partitionFields: Seq[String], mergeKey: String, columns: Seq[String]): MergeQuery =
    MergeQuery(targetName, sourceQuery)
    ++ OnSegment(Map(), mergeKey, partitionFields)
    ++ MatchedAppendOnlyDelete()
    ++ MatchedAppendOnlyUpdate(columns.filterNot(c => c == mergeKey))
    ++ NotMatchedAppendOnlyInsert(columns)

object SynapseLinkBackfillQuery:
  def apply(targetName: String, sourceQuery: String, tablePropertiesSettings: TablePropertiesSettings): OverwriteQuery =
    OverwriteReplaceQuery(sourceQuery, targetName, tablePropertiesSettings)

class SynapseLinkBackfillBatch(batchName: String, batchSchema: ArcaneSchema, targetName: String, archiveName: String, tablePropertiesSettings: TablePropertiesSettings)
  extends StagedBackfillOverwriteBatch:

  override val name: String = batchName
  override val schema: ArcaneSchema = batchSchema

  override def reduceExpr: String =
    // important to note that append-only nature of the source must be taken into account
    // thus, we need identify which of the latest versions were deleted after we have found the latest versions for each `Id` - since for backfill we must exclude deletions
    s"""SELECT * FROM (
       | SELECT * FROM $name ORDER BY ROW_NUMBER() OVER (PARTITION BY ${schema.mergeKey.name} ORDER BY versionnumber DESC) FETCH FIRST 1 ROWS WITH TIES
       |) WHERE coalesce(IsDelete, false) = false""".stripMargin

  override val batchQuery: OverwriteQuery = SynapseLinkBackfillQuery(targetName, reduceExpr, tablePropertiesSettings)

  override def archiveExpr(actualSchema: ArcaneSchema): String =
    s"INSERT INTO $archiveName ${actualSchema.toColumnsExpression} $reduceExpr"

object  SynapseLinkBackfillBatch:
  def apply(batchName: String, batchSchema: ArcaneSchema, targetName: String, archiveName: String, tablePropertiesSettings: TablePropertiesSettings): SynapseLinkBackfillBatch =
    new SynapseLinkBackfillBatch(batchName: String, batchSchema: ArcaneSchema, targetName, archiveName, tablePropertiesSettings)

class SynapseLinkMergeBatch(batchName: String, batchSchema: ArcaneSchema, targetName: String, archiveName: String, tablePropertiesSettings: TablePropertiesSettings, mergeKey: String) extends StagedVersionedBatch with StagedBackfillMergeBatch:
  override val name: String = batchName
  override val schema: ArcaneSchema = batchSchema

  override def reduceExpr: String =
    // for merge query, we must carry over deletions so they can be applied in a MERGE statement by MatchedAppendOnlyDelete
    s"""SELECT * FROM (
       | SELECT * FROM $name ORDER BY ROW_NUMBER() OVER (PARTITION BY ${schema.mergeKey.name} ORDER BY versionnumber DESC) FETCH FIRST 1 ROWS WITH TIES
       |)""".stripMargin

  override val batchQuery: MergeQuery =
    SynapseLinkMergeQuery(targetName = targetName, sourceQuery = reduceExpr, partitionFields = tablePropertiesSettings.partitionFields, mergeKey = mergeKey, columns = schema.map(f => f.name))

  override def archiveExpr(actualSchema: ArcaneSchema): String =
    s"INSERT INTO $archiveName ${actualSchema.toColumnsExpression} $reduceExpr"

object SynapseLinkMergeBatch:
  def apply(batchName: String, batchSchema: ArcaneSchema, targetName: String, archiveName: String, tablePropertiesSettings: TablePropertiesSettings): SynapseLinkMergeBatch =
    new SynapseLinkMergeBatch(batchName, batchSchema, targetName, archiveName, tablePropertiesSettings, batchSchema.mergeKey.name)
