package com.sneaksanddata.arcane.framework
package models.sharding

import models.queries.{OverwriteReplaceQuery, ShardCommitQuery, StreamingBatchQuery}
import services.streaming.base.{JsonWatermark, SourceWatermark, StructuredZStream}

import com.sneaksanddata.arcane.framework.models.settings.EmptyTablePropertiesSettings
import zio.stream.ZStream

/**
 * A shard of data from source to be used by backfills
 */
trait SourceShard:
  val shardId: String
  val combinedTableName: String
  val targetTableName: String

/**
 * A shard of source data that has been successfully bootstrapped and is ready for staging
 */
trait BootstrappedShard extends SourceShard:
  val shardStream: StructuredZStream
  val shardSourceEntityName: String

/**
 * A staged shard contains a chunk of data from source that has been successfully streamed out
 */
trait StagedShard extends SourceShard:
  val shardTableName: String
  val shardSourceEntityName: String
  val commitQuery: StreamingBatchQuery = ShardCommitQuery(combinedTableName, shardTableName)
  
object StagedShard:
  def apply(id: String, shardTableName: String, shardSourceName: String, combinedTableName: String, targetTableName: String): StagedShard = new StagedShard {
    override val shardTableName: String = shardTableName
    override val shardSourceEntityName: String = shardSourceName
    override val shardId: String = id
    override val combinedTableName: String = combinedTableName
    override val targetTableName: String = targetTableName
  }
  extension (shard: BootstrappedShard)
    def toStaged(shardTableName: String): StagedShard = StagedShard(
      shard.shardId,
      shardTableName,
      shard.shardSourceEntityName,
      shard.combinedTableName,
      shard.targetTableName
    )

case class CompletionShard(watermark: JsonWatermark, targetTableName: String, shardSourceEntityName: String) extends StagedShard:
  override val shardId: String = "watermark"
  override val shardTableName: String = ???
  override val combinedTableName: String = ???
  override val commitQuery: StreamingBatchQuery = OverwriteReplaceQuery(s"SELECT * FROM $combinedTableName", targetTableName, EmptyTablePropertiesSettings)

object CompletionShard:
  extension (shard: CompletionShard)
    def toCompleted: CompletedShard = CompletedShard(shard.shardId, shard.combinedTableName, shard.targetTableName)  
  
case class CompletedShard(shardId: String, combinedTableName: String, targetTableName: String) extends StagedShard:
  override val shardTableName: String = ???
  override val shardSourceEntityName: String = ???

