package com.sneaksanddata.arcane.framework
package plugins.synapse

import services.backfill.base.{
  BackfillStateManager,
  BackfillStreamDataProvider,
  ShardFactory,
  ShardedBackfillStreamDataProvider
}
import services.iceberg.base.{SinkPropertyManager, StagingEntityManager, StagingPropertyManager}
import services.metrics.DeclaredMetrics
import services.naming.NameGenerator
import services.streaming.base.StreamDataProvider
import services.streaming.batching.StagedBatchFactory

import models.app.PluginStreamContext
import services.backfill.DefaultBackfillStateManager
import services.streaming.throughput.base.ThroughputShaperBuilder
import services.synapse.{SynapseBatchFactory, SynapseLinkStreamingDataProvider}
import services.synapse.backfill.{
  SynapseBackfillMergeStreamDataProvider,
  SynapseBackfillSourceDataProvider,
  SynapseShardFactory,
  SynapseShardedBackfillStreamDataProvider
}
import services.synapse.base.{SynapseLinkDataProvider, SynapseLinkStreamingSource}

import zio.ZLayer

type SynapseLinkProvidedServices = StagedBatchFactory & ShardFactory & ShardedBackfillStreamDataProvider &
  StreamDataProvider & BackfillStreamDataProvider & BackfillStateManager
type SynapseLinkSourceRequiredServices = SinkPropertyManager & StagingEntityManager & StagingPropertyManager &
  NameGenerator & DeclaredMetrics

object Services:
  lazy val synapseLinkSourceLayer: ZLayer[
    SynapseLinkSourceRequiredServices & SynapseLinkStreamingSource & PluginStreamContext,
    Throwable,
    SynapseLinkProvidedServices
  ] =
    ZLayer.makeSome[
      SynapseLinkSourceRequiredServices & SynapseLinkStreamingSource & PluginStreamContext,
      SynapseLinkProvidedServices
    ](
      // streaming
      SynapseLinkStreamingDataProvider.layer,
      SynapseBatchFactory.layer,
      SynapseShardFactory.layer,
      SynapseLinkDataProvider.layer,

      // backfill
      SynapseBackfillSourceDataProvider.layer,
      SynapseShardedBackfillStreamDataProvider.layer,
      SynapseBackfillMergeStreamDataProvider.layer,
      ThroughputShaperBuilder.layer,
      DefaultBackfillStateManager.layer
    )
