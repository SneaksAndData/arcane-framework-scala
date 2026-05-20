package com.sneaksanddata.arcane.framework
package models.sharding

import models.queries.{OverwriteReplaceQuery, StreamingBatchQuery}
import models.settings.EmptyTablePropertiesSettings
import services.streaming.base.JsonWatermark

case class CompletionShard(
    watermark: String,
    override val shardTableName: String,
    override val targetTableName: String,
    override val shardSourceEntityName: String,
    override val combinedTableName: String
) extends StagedShard:
  override val shardId: String = "watermark"
  // TODO: must be customized per source
  override val commitQuery: StreamingBatchQuery =
    OverwriteReplaceQuery(s"SELECT * FROM $combinedTableName", targetTableName, EmptyTablePropertiesSettings)

object CompletionShard:
  extension (shard: CompletionShard)
    def toCompleted: CompletedShard =
      CompletedShard(shard.shardId, shard.combinedTableName, shard.targetTableName, shard.shardSourceEntityName)
