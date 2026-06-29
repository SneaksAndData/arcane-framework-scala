package com.sneaksanddata.arcane.framework
package services.pushstream.backfill

/** Backfilling is not supported by PullStream plugin. This module provides No-op implementations for the necessary
  * backfill layers that are required by 'GenericStreamRunnerService'.
  */

import com.sneaksanddata.arcane.framework.models.sharding.{BootstrappedShard, CompletionShard, StagedShard}
import com.sneaksanddata.arcane.framework.services.backfill.base.{
  BackfillStreamDataProvider,
  ShardFactory,
  ShardedBackfillStreamDataProvider
}
import com.sneaksanddata.arcane.framework.services.streaming.base.{JsonWatermark, StructuredZStream}
import zio.stream.ZStream
import zio.{Task, ULayer, ZIO, ZLayer}

private def unsupported(name: String): Task[Nothing] =
  ZIO.fail(new UnsupportedOperationException(s"$name is not supported by the pull stream plugin"))

object NoopBackfillStreamDataProvider extends BackfillStreamDataProvider:
  override def stream: ZStream[Any, Throwable, StructuredZStream] =
    ZStream.fromZIO(unsupported("BackfillStreamDataProvider.stream"))

  val layer: ULayer[BackfillStreamDataProvider] = ZLayer.succeed(this)

object NoopShardedBackfillStreamDataProvider extends ShardedBackfillStreamDataProvider:
  override def backfillStream: Task[(stream: ZStream[Any, Throwable, BootstrappedShard], watermark: JsonWatermark)] =
    unsupported("ShardedBackfillStreamDataProvider.backfillStream")

  val layer: ULayer[ShardedBackfillStreamDataProvider] = ZLayer.succeed(this)

object NoopShardFactory extends ShardFactory:
  override def createStagedShard(shard: BootstrappedShard): Task[StagedShard] =
    unsupported("ShardFactory.createStagedShard")

  override def createCompletionShard(shard: StagedShard, watermark: String): Task[CompletionShard] =
    unsupported("ShardFactory.createCompletionShard")

  val layer: ULayer[ShardFactory] = ZLayer.succeed(this)
