package com.sneaksanddata.arcane.framework
package models.sharding

import models.queries.{OverwriteReplaceQuery, StreamingBatchQuery}
import models.settings.EmptyTablePropertiesSettings
import services.streaming.base.JsonWatermark

case class CompletionShard(
    watermark: String,
    override val targetTableName: String,
    override val shardSourceEntityName: String,
    override val combinedTableName: String,
    override val commitQuery: StreamingBatchQuery
) extends StagedShard

object CompletionShard:
  extension (shard: CompletionShard)
    def toCompleted: CompletedShard =
      CompletedShard(shard.combinedTableName, shard.targetTableName, shard.shardSourceEntityName)
