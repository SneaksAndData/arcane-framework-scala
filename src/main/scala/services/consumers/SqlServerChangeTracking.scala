package com.sneaksanddata.arcane.framework
package services.consumers

import models.querygen.{MergeQuery, MergeQueryCommons, MergeQuerySegment, OnSegment, OverwriteQuery, WhenMatchedDelete, WhenMatchedUpdate, WhenNotMatchedInsert}
import models.ArcaneSchema

object WhenMatchedDelete:
  def apply(): WhenMatchedDelete = new WhenMatchedDelete {
    override val segmentCondition: Option[String] = Some(s"${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_OPERATION = 'D'")
  }

object WhenMatchedUpdate {
  def apply(cols: Seq[String]): WhenMatchedUpdate = new WhenMatchedUpdate {
    override val segmentCondition: Option[String] = Some(s"${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_OPERATION != 'D' AND ${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_VERSION > ${MergeQueryCommons.TARGET_ALIAS}.SYS_CHANGE_VERSION")
    override val columns: Seq[String] = cols
  }
}

object WhenNotMatchedInsert {
  def apply(cols: Seq[String]): WhenNotMatchedInsert = new WhenNotMatchedInsert {
    override val columns: Seq[String] = cols
    override val segmentCondition: Option[String] = Some(s"${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_OPERATION != 'D'")
  }
}

object SqlServerChangeTrackingMergeQuery:
  def apply(targetName: String, sourceQuery: String, partitionValues: Map[String, List[String]], mergeKey: String, columns: Seq[String]): MergeQuery =
    MergeQuery(targetName, sourceQuery)
      ++ OnSegment(partitionValues, mergeKey)
      ++ WhenMatchedDelete()
      ++ WhenMatchedUpdate(columns.filterNot(c => c == mergeKey))
      ++ WhenNotMatchedInsert(columns)

object SqlServerChangeTrackingBackfillQuery:
  def apply(targetName: String, sourceQuery: String): OverwriteQuery = OverwriteQuery(sourceQuery, targetName)

class SqlServerChangeTrackingBackfillBatch(batchName: String, batchSchema: ArcaneSchema, targetName: String) extends StagedBackfillBatch:
  override val name: String = batchName
  override val schema: ArcaneSchema = batchSchema

  override def reduceExpr: String =
    s"""SELECT * FROM $name AS ${MergeQueryCommons.SOURCE_ALIAS} WHERE ${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_OPERATION != 'D'""".stripMargin

  override val batchQuery: OverwriteQuery = SqlServerChangeTrackingBackfillQuery(targetName, reduceExpr)

  def archiveExpr: String = s"INSERT OVERWRITE ${targetName}_stream_archive $reduceExpr"

object  SqlServerChangeTrackingBackfillBatch:
  /**
   *
   */
  def apply(batchName: String, batchSchema: ArcaneSchema, targetName: String): StagedBackfillBatch = new SqlServerChangeTrackingBackfillBatch(batchName: String, batchSchema: ArcaneSchema, targetName)

class SqlServerChangeTrackingMergeBatch(batchName: String, batchSchema: ArcaneSchema, targetName: String, partitionValues: Map[String, List[String]], mergeKey: String) extends StagedVersionedBatch:
  override val name: String = batchName
  override val schema: ArcaneSchema = batchSchema

  override def reduceExpr: String =
    s"""
       |SELECT
       |${MergeQueryCommons.SOURCE_ALIAS}.*
       |FROM $name AS ${MergeQueryCommons.SOURCE_ALIAS} inner join (SELECT $mergeKey, MAX(SYS_CHANGE_VERSION) AS LATEST_VERSION FROM $name GROUP BY $mergeKey) as v
       |on ${MergeQueryCommons.SOURCE_ALIAS}.$mergeKey = v.$mergeKey AND ${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_VERSION = v.LATEST_VERSION
       |""".stripMargin

  override val batchQuery: MergeQuery =
    SqlServerChangeTrackingMergeQuery(targetName = targetName, sourceQuery = reduceExpr, partitionValues = partitionValues, mergeKey = mergeKey, columns = schema.map(f => f.name))

  def archiveExpr: String = s"INSERT INTO ${targetName}_stream_archive $reduceExpr"

object SqlServerChangeTrackingMergeBatch:
  def apply(batchName: String, batchSchema: ArcaneSchema, targetName: String, partitionValues: Map[String, List[String]]): StagedVersionedBatch =
    new SqlServerChangeTrackingMergeBatch(batchName: String, batchSchema: ArcaneSchema, targetName: String, partitionValues: Map[String, List[String]], batchSchema.mergeKey.name)
