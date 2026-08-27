package com.sneaksanddata.arcane.framework
package plugins.pullstream

import models.app.PluginStreamContext
import services.backfill.DefaultBackfillStateManager
import services.backfill.base.{
  BackfillStateManager,
  BackfillStreamDataProvider,
  ShardFactory,
  ShardedBackfillStreamDataProvider
}
import services.iceberg.base.{SinkPropertyManager, StagingEntityManager, StagingPropertyManager}
import services.metrics.DeclaredMetrics
import services.naming.NameGenerator
import services.pullstream.backfill.PullStreamBackfillLayers
import services.pullstream.{
  PullStreamSourceDataProvider,
  PullStreamStagedBatchFactory,
  PullStreamStreamingDataProvider,
  PullStreamingSource
}
import services.streaming.base.StreamDataProvider
import services.streaming.batching.StagedBatchFactory
import services.streaming.throughput.base.ThroughputShaperBuilder

import zio.ZLayer

type PullStreamRequiredServices = SinkPropertyManager & StagingEntityManager & StagingPropertyManager & NameGenerator &
  DeclaredMetrics
type PullStreamProvidedServices = StagedBatchFactory & ShardFactory & ShardedBackfillStreamDataProvider &
  StreamDataProvider & BackfillStreamDataProvider & BackfillStateManager

object Services:
  lazy val sourceLayer: ZLayer[
    PullStreamRequiredServices & PullStreamingSource & PluginStreamContext,
    Throwable,
    PullStreamProvidedServices
  ] = ZLayer.makeSome[
    PullStreamRequiredServices & PullStreamingSource & PluginStreamContext,
    PullStreamProvidedServices
  ](
    PullStreamSourceDataProvider.layer,
    PullStreamStreamingDataProvider.layer,
    PullStreamStagedBatchFactory.layer,

    // backfill and sharding are not supported by the pull stream plugin
    PullStreamBackfillLayers.backfillStreamDataProvider,
    PullStreamBackfillLayers.shardedBackfillStreamDataProvider,
    PullStreamBackfillLayers.shardFactory,
    ThroughputShaperBuilder.layer,
    DefaultBackfillStateManager.layer
  )
