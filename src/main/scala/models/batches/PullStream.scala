package com.sneaksanddata.arcane.framework
package models.batches

import models.schemas.ArcaneSchema
import models.settings.{EmptyTablePropertiesSettings, TablePropertiesSettings}

import models.queries.{MergeQuery, MergeQueryCommons, OnSegment, WhenMatchedUpdate, WhenNotMatchedInsert}

object PullStreamChangeTrackingMergeQuery:
  def empty: MergeQuery =
    MergeQuery("", "")
      ++ OnSegment(Map.empty, "", Seq.empty)
      ++ WhenNotMatchedInsert(Seq.empty)

  private def matchedUpdate(cols: Seq[String], versionField: String): WhenMatchedUpdate =
    new WhenMatchedUpdate {
      override val segmentCondition: Option[String] = Some(
        s"${MergeQueryCommons.SOURCE_ALIAS}.$versionField > ${MergeQueryCommons.TARGET_ALIAS}.$versionField"
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
      mergeKey: String,
      columns: Seq[String],
      versionFieldName: String
  ): MergeQuery = {
    MergeQuery(targetName, sourceQuery)
      ++ OnSegment(Map.empty, mergeKey, partitionFields.filterNot(_ == mergeKey))
      ++ matchedUpdate(columns.filterNot(_ == mergeKey), versionFieldName)
      ++ NotMatchedInsert(columns)
  }

class PullStreamChangeTrackingMergeBatch(
    batchName: String,
    batchSchema: ArcaneSchema,
    targetName: String,
    tablePropertiesSettings: TablePropertiesSettings,
    mergeKey: String,
    versionFieldName: String
) extends StagedVersionedBatch
    with MergeableBatch:

  override val name: String            = batchName
  override val schema: ArcaneSchema    = batchSchema
  override val targetTableName: String = targetName

  override def reduceExpr: String =
    s"""SELECT * FROM (
       |  SELECT * FROM $name
       |  ORDER BY ROW_NUMBER() OVER (
       |    PARTITION BY ${schema.mergeKey.name}
       |    ORDER BY $versionFieldName DESC
       |  ) FETCH FIRST 1 ROWS WITH TIES
       |)""".stripMargin

  override val batchQuery: MergeQuery =
    if schema.isEmpty then PullStreamChangeTrackingMergeQuery.empty
    else
      PullStreamChangeTrackingMergeQuery(
        targetName = targetName,
        sourceQuery = reduceExpr,
        partitionFields = Seq.empty,
        mergeKey = mergeKey,
        columns = schema.map(f => f.name),
        versionFieldName = versionFieldName
      )

  override val completedWatermarkValue: Option[String] = None

object PullStreamChangeTrackingMergeBatch:
  def apply(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName: String,
      tablePropertiesSettings: TablePropertiesSettings,
      versionFieldName: String
  ): PullStreamChangeTrackingMergeBatch =
    new PullStreamChangeTrackingMergeBatch(
      batchName,
      batchSchema,
      targetName,
      tablePropertiesSettings,
      batchSchema.mergeKey.name,
      versionFieldName
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
