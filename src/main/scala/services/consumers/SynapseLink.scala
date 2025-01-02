package com.sneaksanddata.arcane.framework
package services.consumers

import models.ArcaneSchema
import models.querygen.{MergeQuery, MergeQueryCommons, OnSegment, OverwriteQuery, WhenMatchedDelete, WhenMatchedUpdate, WhenNotMatchedInsert}

object MatchedAppendOnlyDelete:
  def apply(): WhenMatchedDelete = new WhenMatchedDelete {
    override val segmentCondition: Option[String] = Some(s"${MergeQueryCommons.SOURCE_ALIAS}.IsDelete = true")
  }

object MatchedAppendOnlyUpdate {
  def apply(cols: Seq[String]): WhenMatchedUpdate = new WhenMatchedUpdate {
    override val segmentCondition: Option[String] = Some(s"${MergeQueryCommons.SOURCE_ALIAS}.IsDelete = false AND ${MergeQueryCommons.SOURCE_ALIAS}.versionnumber > ${MergeQueryCommons.TARGET_ALIAS}.versionnumber")
    override val columns: Seq[String] = cols
  }
}

object NotMatchedAppendOnlyInsert {
  def apply(cols: Seq[String]): WhenNotMatchedInsert = new WhenNotMatchedInsert {
    override val columns: Seq[String] = cols
    override val segmentCondition: Option[String] = Some(s"${MergeQueryCommons.SOURCE_ALIAS}.IsDelete = false")
  }
}

object SynapseLinkMergeQuery:
  def apply(targetName: String, sourceQuery: String, partitionValues: Map[String, List[String]], mergeKey: String, columns: Seq[String]): MergeQuery =
    MergeQuery(targetName, sourceQuery)
    ++ OnSegment(partitionValues, mergeKey)
    ++ MatchedAppendOnlyDelete()
    ++ MatchedAppendOnlyUpdate(columns.filterNot(c => c == mergeKey))
    ++ NotMatchedAppendOnlyInsert(columns)

object SynapseLinkBackfillQuery:
  def apply(targetName: String, sourceQuery: String): OverwriteQuery = OverwriteQuery(sourceQuery, targetName)

class SynapseLinkBackfillBatch(batchName: String, batchSchema: ArcaneSchema, targetName: String) extends StagedBackfillBatch:
  override val name: String = batchName
  override val schema: ArcaneSchema = batchSchema

  override def reduceExpr: String =
    // important to note that append-only nature of the source must be taken into account
    // thus, we need identify which of the latest versions were deleted after we have found the latest versions for each `Id` - since for backfill we must exclude deletions
    s"""SELECT * FROM (
       | SELECT * FROM $name ORDER BY ROW_NUMBER() OVER (PARTITION BY Id ORDER BY versionnumber DESC) FETCH FIRST 1 ROWS WITH TIES
       |) WHERE IsDelete = false""".stripMargin

  override val batchQuery: OverwriteQuery = SynapseLinkBackfillQuery(targetName, reduceExpr)

  def archiveExpr: String = s"INSERT OVERWRITE ${targetName}_stream_archive $reduceExpr"

object  SynapseLinkBackfillBatch:
  /**
   *
   */
  def apply(batchName: String, batchSchema: ArcaneSchema, targetName: String): StagedBackfillBatch = new SynapseLinkBackfillBatch(batchName: String, batchSchema: ArcaneSchema, targetName)

class SynapseLinkMergeBatch(batchName: String, batchSchema: ArcaneSchema, targetName: String, partitionValues: Map[String, List[String]], mergeKey: String) extends StagedVersionedBatch:
  override val name: String = batchName
  override val schema: ArcaneSchema = batchSchema

  override def reduceExpr: String =
    // for merge query, we must carry over deletions so they can be applied in a MERGE statement by MatchedAppendOnlyDelete
    s"""SELECT * FROM (
       | SELECT * FROM $name ORDER BY ROW_NUMBER() OVER (PARTITION BY Id ORDER BY versionnumber DESC) FETCH FIRST 1 ROWS WITH TIES
       |)""".stripMargin

  override val batchQuery: MergeQuery =
    SynapseLinkMergeQuery(targetName = targetName, sourceQuery = reduceExpr, partitionValues = partitionValues, mergeKey = mergeKey, columns = schema.map(f => f.name))

  def archiveExpr: String = s"INSERT INTO ${targetName}_stream_archive $reduceExpr"

object SynapseLinkMergeBatch:
  def apply(batchName: String, batchSchema: ArcaneSchema, targetName: String, partitionValues: Map[String, List[String]]): StagedVersionedBatch =
    new SynapseLinkMergeBatch(batchName: String, batchSchema: ArcaneSchema, targetName: String, partitionValues: Map[String, List[String]], batchSchema.mergeKey.name)
