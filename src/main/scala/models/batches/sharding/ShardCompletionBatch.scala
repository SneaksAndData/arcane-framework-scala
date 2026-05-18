package com.sneaksanddata.arcane.framework
package models.batches.sharding

import models.batches.StagedBatch
import models.queries.{OverwriteQuery, OverwriteReplaceQuery}
import models.settings.EmptyTablePropertiesSettings
import services.streaming.base.JsonWatermark

class ShardCompletionBatch(watermark: JsonWatermark, targetName: String, combinedTableName: String) extends StagedBatch:
  override type Query = OverwriteQuery
  override val name: String = "watermark"
  override val completedWatermarkValue: Option[String] = Some(watermark.toJson)
  override val batchQuery: OverwriteQuery = OverwriteReplaceQuery(s"SELECT * FROM $combinedTableName", targetName, EmptyTablePropertiesSettings)
