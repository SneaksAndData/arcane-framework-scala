package com.sneaksanddata.arcane.framework
package models.batches

import models.schemas.{ArcaneSchema, MergeKeyField, VersionField}
import models.settings.{EmptyTablePropertiesSettings, TablePropertiesSettings}
import models.queries.{MergeQuery, MergeQueryCommons, OnSegment, WhenMatchedUpdate, WhenNotMatchedInsert}

object PullStreamChangeTrackingMergeQuery:
  def empty: MergeQuery =
    MergeQuery("", "")
      ++ OnSegment(Map.empty, "", Seq.empty)
      ++ WhenNotMatchedInsert(Seq.empty)

  private def matchedUpdate(cols: Seq[String]): WhenMatchedUpdate =
    new WhenMatchedUpdate {
      override val segmentCondition: Option[String] = Some(
        s"${MergeQueryCommons.SOURCE_ALIAS}.${VersionField.name} > ${MergeQueryCommons.TARGET_ALIAS}.${VersionField.name}"
      )
      override val columns: Seq[String] = cols
    }

  private def notMatchedInsert(cols: Seq[String]): WhenNotMatchedInsert =
    new WhenNotMatchedInsert {
      override val columns: Seq[String]             = cols
      override val segmentCondition: Option[String] = None
    }

  def apply(
      targetName: String,
      sourceQuery: String,
      partitionFields: Seq[String],
      columns: Seq[String]
  ): MergeQuery = {
    MergeQuery(targetName, sourceQuery)
      ++ OnSegment(Map.empty, MergeKeyField.name, partitionFields.filterNot(_ == MergeKeyField.name))
      ++ matchedUpdate(columns.filterNot(_ == MergeKeyField.name))
      ++ NotMatchedInsert(columns)
  }

class PullStreamChangeTrackingMergeBatch(
    batchName: String,
    batchSchema: ArcaneSchema,
    targetName: String,
    tablePropertiesSettings: TablePropertiesSettings
) extends StagedVersionedBatch
    with MergeableBatch:

  override val name: String            = batchName
  override val schema: ArcaneSchema    = batchSchema
  override val targetTableName: String = targetName

  override def reduceExpr: String =
    s"""SELECT * FROM (
       |  SELECT * FROM $name
       |  ORDER BY ROW_NUMBER() OVER (
       |    PARTITION BY ${MergeKeyField.name}
       |    ORDER BY ${VersionField.name} DESC
       |  ) FETCH FIRST 1 ROWS WITH TIES
       |)""".stripMargin

  override val batchQuery: MergeQuery =
    if schema.isEmpty then PullStreamChangeTrackingMergeQuery.empty
    else
      PullStreamChangeTrackingMergeQuery(
        targetName = targetName,
        sourceQuery = reduceExpr,
        partitionFields = Seq.empty,
        columns = schema.map(f => f.name)
      )

  override val completedWatermarkValue: Option[String] = None

object PullStreamChangeTrackingMergeBatch:
  def apply(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName: String,
      tablePropertiesSettings: TablePropertiesSettings
  ): PullStreamChangeTrackingMergeBatch =
    new PullStreamChangeTrackingMergeBatch(
      batchName,
      batchSchema,
      targetName,
      tablePropertiesSettings
    )

class PullStreamChangeTrackingWatermarkOnlyBatch(
    targetName: String,
    watermarkValue: String
) extends WatermarkOnlyBatch:

  override val name: String            = "watermark"
  override val schema: ArcaneSchema    = ArcaneSchema.empty()
  override val targetTableName: String = targetName

  override def reduceExpr: String = ""

  override val batchQuery: MergeQuery = PullStreamChangeTrackingMergeQuery.empty

  override val completedWatermarkValue: Option[String] = Some(watermarkValue)

object PullStreamChangeTrackingWatermarkOnlyBatch:
  def apply(targetName: String, watermarkValue: String): PullStreamChangeTrackingWatermarkOnlyBatch =
    new PullStreamChangeTrackingWatermarkOnlyBatch(targetName, watermarkValue)
