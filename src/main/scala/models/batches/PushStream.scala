package com.sneaksanddata.arcane.framework
package models.batches

import models.schemas.ArcaneSchema
import models.settings.{EmptyTablePropertiesSettings, TablePropertiesSettings}

import models.queries.{MergeQuery, MergeQueryCommons, OnSegment, WhenMatchedUpdate, WhenNotMatchedInsert}

object PushStreamChangeTrackingMergeQuery:
  def empty: MergeQuery =
    MergeQuery("", "")
      ++ OnSegment(Map.empty, "", Seq.empty)
      ++ WhenNotMatchedInsert(Seq.empty)

  def apply(
      targetName: String,
      sourceQuery: String,
      partitionFields: Seq[String],
      mergeKey: String,
      columns: Seq[String]
  ): MergeQuery = {
    MergeQuery(targetName, sourceQuery)
      ++ OnSegment(Map.empty, mergeKey, partitionFields.filterNot(_ == mergeKey))
      ++ MatchedUpdate(columns.filterNot(_ == mergeKey))
      ++ NotMatchedInsert(columns)
  }

class PushStreamChangeTrackingMergeBatch(
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
    if schema.isEmpty then PushStreamChangeTrackingMergeQuery.empty
    else
      PushStreamChangeTrackingMergeQuery(
        targetName = targetName,
        sourceQuery = reduceExpr,
        partitionFields = Seq.empty,
        mergeKey = mergeKey,
        columns = schema.map(f => f.name)
      )

  override val completedWatermarkValue: Option[String] = None

object PushStreamChangeTrackingMergeBatch:
  def apply(
      batchName: String,
      batchSchema: ArcaneSchema,
      targetName: String,
      tablePropertiesSettings: TablePropertiesSettings,
      versionFieldName: String
  ): PushStreamChangeTrackingMergeBatch =
    new PushStreamChangeTrackingMergeBatch(
      batchName,
      batchSchema,
      targetName,
      tablePropertiesSettings,
      batchSchema.mergeKey.name,
      versionFieldName
    )

class PushStreamChangeTrackingWatermarkOnlyBatch(
    targetName: String,
    watermarkValue: String
) extends WatermarkOnlyBatch:

  override val name: String            = "watermark"
  override val schema: ArcaneSchema    = ArcaneSchema.empty()
  override val targetTableName: String = targetName

  override def reduceExpr: String = ""

  override val batchQuery: MergeQuery = PushStreamChangeTrackingMergeQuery.empty

  override val completedWatermarkValue: Option[String] = Some(watermarkValue)

object PushStreamChangeTrackingWatermarkOnlyBatch:
  def apply(targetName: String, watermarkValue: String): PushStreamChangeTrackingWatermarkOnlyBatch =
    new PushStreamChangeTrackingWatermarkOnlyBatch(targetName, watermarkValue)
