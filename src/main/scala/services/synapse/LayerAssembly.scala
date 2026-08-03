package com.sneaksanddata.arcane.framework
package services.synapse

import models.app.PluginStreamContext
import services.backfill.DefaultBackfillStateManager
import services.iceberg.base.SinkPropertyManager
import services.metrics.DeclaredMetrics
import services.naming.NameGenerator
import services.streaming.base.StreamDataProvider
import services.streaming.throughput.base.ThroughputShaperBuilder
import services.synapse.backfill.{
  SynapseBackfillMergeStreamDataProvider,
  SynapseBackfillSourceDataProvider,
  SynapseShardedBackfillStreamDataProvider
}
import services.synapse.base.{SynapseLinkDataProvider, SynapseLinkStreamingSource}

import zio.ZLayer

object LayerAssembly:
  type SynapseLinkServices = StreamDataProvider & SynapseLinkDataProvider & SynapseBatchFactory &
    SynapseBackfillSourceDataProvider & SynapseShardedBackfillStreamDataProvider &
    SynapseBackfillMergeStreamDataProvider
  type FrameworkRequiredServices = SinkPropertyManager & DefaultBackfillStateManager & NameGenerator &
    PluginStreamContext & DeclaredMetrics

  lazy val synapseLinkSourceLayer
      : ZLayer[FrameworkRequiredServices & SynapseLinkStreamingSource, Throwable, SynapseLinkServices] =
    ZLayer.makeSome[FrameworkRequiredServices & SynapseLinkStreamingSource, SynapseLinkServices](
      // streaming
      SynapseLinkStreamingDataProvider.layer,
      SynapseBatchFactory.layer,
      SynapseLinkDataProvider.layer,

      // backfill
      SynapseBackfillSourceDataProvider.layer,
      SynapseShardedBackfillStreamDataProvider.layer,
      SynapseBackfillMergeStreamDataProvider.layer,
      ThroughputShaperBuilder.layer
    )
