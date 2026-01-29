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

object WhenMatchedDelete:
  def apply(): WhenMatchedDelete = new WhenMatchedDelete {
    override val segmentCondition: Option[String] = Some(
      s"${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_OPERATION = 'D'"
    )
  }

object WhenMatchedUpdate {
  def apply(cols: Seq[String]): WhenMatchedUpdate = new WhenMatchedUpdate {
    override val segmentCondition: Option[String] = Some(
      s"${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_OPERATION != 'D' AND ${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_VERSION > ${MergeQueryCommons.TARGET_ALIAS}.SYS_CHANGE_VERSION"
    )
    override val columns: Seq[String] = cols
  }
}

object WhenNotMatchedInsert {
  def apply(cols: Seq[String]): WhenNotMatchedInsert = new WhenNotMatchedInsert {
    override val columns: Seq[String] = cols
    override val segmentCondition: Option[String] = Some(
      s"${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_OPERATION != 'D'"
    )
  }
}

object SqlServerChangeTrackingMergeQuery:
  def apply(
      targetName: String,
      sourceQuery: String,
      partitionFields: Seq[String],
      mergeKey: String,
      columns: Seq[String]
  ): MergeQuery =
    MergeQuery(targetName, sourceQuery)
      ++ OnSegment(Map(), mergeKey, partitionFields.filterNot(c => c == mergeKey))
      ++ WhenMatchedDelete()
      ++ WhenMatchedUpdate(columns.filterNot(c => c == mergeKey))
      ++ WhenNotMatchedInsert(columns)

object SqlServerChangeTrackingBackfillQuery:
  def apply(targetName: String, sourceQuery: String, tablePropertiesSettings: TablePropertiesSettings): OverwriteQuery =
    OverwriteReplaceQuery(sourceQuery, targetName, tablePropertiesSettings)

class SqlServerChangeTrackingBackfillBatch(
    batchName: String,
    batchSchema: ArcaneSchema,
    targetName: String,
    tablePropertiesSettings: TablePropertiesSettings,
    watermarkValue: Option[String]
) extends StagedBackfillOverwriteBatch:

  override val name: String            = batchName
  override val schema: ArcaneSchema    = batchSchema
  override val targetTableName: String = targetName

  override def reduceExpr: String =
    s"""SELECT * FROM $name AS ${MergeQueryCommons.SOURCE_ALIAS} WHERE ${MergeQueryCommons.SOURCE_ALIAS}.SYS_CHANGE_OPERATION != 'D'""".stripMargin

  override val batchQuery: OverwriteQuery =
    SqlServerChangeTrackingBackfillQuery(targetName, reduceExpr, tablePropertiesSettings)

  override val completedWatermarkValue: Option[String] = watermarkValue

object SqlServerChangeTrackingBackfillBatch:
  /** */
  def apply(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName: String,
      tablePropertiesSettings: TablePropertiesSettings,
      watermarkValue: Option[String]
  ): StagedBackfillOverwriteBatch =
    new SqlServerChangeTrackingBackfillBatch(
      batchName,
      batchSchema,
      targetName,
      tablePropertiesSettings,
      watermarkValue
    )

class SqlServerChangeTrackingMergeBatch(
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
    s"""SELECT * FROM (
     |SELECT * FROM $name ORDER BY ROW_NUMBER() OVER (PARTITION BY ${schema.mergeKey.name} ORDER BY SYS_CHANGE_VERSION DESC) FETCH FIRST 1 ROWS WITH TIES
     |)""".stripMargin

  override val batchQuery: MergeQuery =
    SqlServerChangeTrackingMergeQuery(
      targetName = targetName,
      sourceQuery = reduceExpr,
      partitionFields = tablePropertiesSettings.partitionFields,
      mergeKey = mergeKey,
      columns = schema.map(f => f.name)
    )

  override val completedWatermarkValue: Option[String] = watermarkValue

object SqlServerChangeTrackingMergeBatch:
  def empty(watermarkValue: Option[String]): SqlServerChangeTrackingMergeBatch = new SqlServerChangeTrackingMergeBatch(
    "",
    ArcaneSchema.empty(),
    "",
    EmptyTablePropertiesSettings,
    "",
    watermarkValue
  )
  def apply(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName: String,
      tablePropertiesSettings: TablePropertiesSettings,
      watermarkValue: Option[String]
  ): SqlServerChangeTrackingMergeBatch =
    new SqlServerChangeTrackingMergeBatch(
      batchName,
      batchSchema,
      targetName,
      tablePropertiesSettings,
      batchSchema.mergeKey.name,
      watermarkValue
    )
