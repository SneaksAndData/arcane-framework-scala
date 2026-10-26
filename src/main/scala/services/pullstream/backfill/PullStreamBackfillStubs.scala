package com.sneaksanddata.arcane.framework
package services.pullstream.backfill

import com.sneaksanddata.arcane.framework.models.sharding.{BootstrappedShard, CompletionShard, StagedShard}
import com.sneaksanddata.arcane.framework.services.backfill.base.{
  BackfillStreamDataProvider,
  ShardFactory,
  ShardedBackfillStreamDataProvider
}
import com.sneaksanddata.arcane.framework.services.streaming.base.{JsonWatermark, StructuredZStream}
import com.sneaksanddata.arcane.framework.exceptions.unsupported
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlogWarning
import com.sneaksanddata.arcane.framework.services.pullstream.versioning.PullStreamWatermark
import zio.stream.ZStream
import zio.{Task, ULayer, ZLayer}

/** Names the component in the log messages and failures raised by the stubs below. */
private val pullStreamPlugin = "the pull stream plugin"

/** Backfilling is not supported by PullStream plugin. This module provides No-op implementations for the necessary
  * backfill layers that are required by 'GenericStreamRunnerService'.
  */
object NoopBackfillStreamDataProvider extends BackfillStreamDataProvider:
  override def stream: ZStream[Any, Throwable, StructuredZStream] =
    ZStream.fromZIO(
      zlogWarning(s"Backfilling is not supported by $pullStreamPlugin, BackfillStreamDataProvider.stream is a no-op")
    ) *> ZStream.empty

/** Backfilling and sharding is not supported by PullStream plugin. Used by the backfill OVERWRITE graph; it yields no
  * shards, so the watermark is never committed and [[NoopShardFactory]] is never invoked.
  */
object NoopShardedBackfillStreamDataProvider extends ShardedBackfillStreamDataProvider:
  override def backfillStream: Task[(stream: ZStream[Any, Throwable, BootstrappedShard], watermark: JsonWatermark)] =
    zlogWarning(
      s"Backfilling is not supported by $pullStreamPlugin, ShardedBackfillStreamDataProvider.backfillStream is a no-op"
    ).as((stream = ZStream.empty, watermark = PullStreamWatermark.epoch))

/** Sharding is not supported by pullstream.
  */
object NoopShardFactory extends ShardFactory:
  override def createStagedShard(shard: BootstrappedShard): Task[StagedShard] =
    unsupported("ShardFactory.createStagedShard", pullStreamPlugin)

  override def createCompletionShard(shard: StagedShard, watermark: String): Task[CompletionShard] =
    unsupported("ShardFactory.createCompletionShard", pullStreamPlugin)

object PullStreamBackfillLayers:
  val backfillStreamDataProvider: ULayer[BackfillStreamDataProvider] =
    ZLayer.succeed(NoopBackfillStreamDataProvider)

  val shardedBackfillStreamDataProvider: ULayer[ShardedBackfillStreamDataProvider] =
    ZLayer.succeed(NoopShardedBackfillStreamDataProvider)

  val shardFactory: ULayer[ShardFactory] = ZLayer.succeed(NoopShardFactory)
