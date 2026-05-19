package com.sneaksanddata.arcane.framework
package models.sharding

import models.queries.{OverwriteReplaceQuery, ShardCommitQuery, StreamingBatchQuery}
import services.streaming.base.{JsonWatermark, SourceWatermark, StructuredZStream}

import com.sneaksanddata.arcane.framework.models.settings.EmptyTablePropertiesSettings
import upickle.ReadWriter
import zio.stream.ZStream

/** A shard of data from source to be used by backfills
  */
trait SourceShard:
  val shardId: String
  val combinedTableName: String
  val targetTableName: String

  val shardTableName: String = s"${sys.env.getOrElse("STREAMCONTEXT__STREAM_ID", "undefined").toLowerCase}_${shardId.replace("-", "_")}"

/** A shard of source data that has been successfully bootstrapped and is ready for staging
  */
trait BootstrappedShard extends SourceShard:
  val shardStream: StructuredZStream
  val shardSourceEntityName: String

/** A staged shard contains a chunk of data from source that has been successfully streamed out
  */
trait StagedShard extends SourceShard:
  val shardSourceEntityName: String
  val commitQuery: StreamingBatchQuery = ShardCommitQuery(combinedTableName, shardTableName)

object StagedShard:
  def apply(
      id: String,
      shardSourceName: String,
      combinedTableName: String,
      targetTableName: String
  ): StagedShard = new StagedShard {
    override val shardSourceEntityName: String = shardSourceName
    override val shardId: String               = id
    override val combinedTableName: String     = combinedTableName
    override val targetTableName: String       = targetTableName
  }
  extension (shard: BootstrappedShard)
    def toStaged: StagedShard = StagedShard(
      shard.shardId,
      shard.shardSourceEntityName,
      shard.combinedTableName,
      shard.targetTableName
    )

case class CompletionShard(watermark: JsonWatermark, targetTableName: String, shardSourceEntityName: String)
    extends StagedShard derives ReadWriter:
  override val shardId: String           = "watermark"
  override val combinedTableName: String = ???
  // TODO: must be customized per source
  override val commitQuery: StreamingBatchQuery =
    OverwriteReplaceQuery(s"SELECT * FROM $combinedTableName", targetTableName, EmptyTablePropertiesSettings)

object CompletionShard:
  extension (shard: CompletionShard)
    def toCompleted: CompletedShard = CompletedShard(shard.shardId, shard.combinedTableName, shard.targetTableName)

case class CompletedShard(shardId: String, combinedTableName: String, targetTableName: String) extends StagedShard derives ReadWriter:
  override val shardSourceEntityName: String = ???
