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
import zio.stream.ZStream
import zio.{Task, ULayer, ZLayer}

/** Names the component in the failures raised by the stubs below. */
private val pullStreamPlugin = "the pull stream plugin"

/** Backfilling is not supported by PullStream plugin. This module provides No-op implementations for the necessary
  * backfill layers that are required by 'GenericStreamRunnerService'.
  */
object NoopBackfillStreamDataProvider extends BackfillStreamDataProvider:
  override def stream: ZStream[Any, Throwable, StructuredZStream] =
    ZStream.fromZIO(unsupported("BackfillStreamDataProvider.stream", pullStreamPlugin))

/** Backfilling and sharding is not supported by PullStream plugin.
  */
object NoopShardedBackfillStreamDataProvider extends ShardedBackfillStreamDataProvider:
  override def backfillStream: Task[(stream: ZStream[Any, Throwable, BootstrappedShard], watermark: JsonWatermark)] =
    unsupported("ShardedBackfillStreamDataProvider.backfillStream", pullStreamPlugin)

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
