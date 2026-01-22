package com.sneaksanddata.arcane.framework
package tests.services.streaming.processors.utils

import models.batches.{MergeableBatch, StagedVersionedBatch}
import models.queries.MergeQuery
import models.schemas.ArcaneSchema
import models.settings.TablePropertiesSettings

class TestStageVersionedBatch(
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

  override def reduceExpr: String = """SELECT 1"""

  override val batchQuery: MergeQuery = new MergeQuery(baseQuery = "SELETT 1", segments = List.empty)

  override val completedWatermarkValue: Option[String] = watermarkValue
