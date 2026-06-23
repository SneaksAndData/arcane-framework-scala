package com.sneaksanddata.arcane.framework
package services.synapse.base

import models.app.PluginStreamContext
import models.settings.sink.SinkSettings
import models.settings.sources.SourceBufferingSettings
import services.iceberg.base.SinkPropertyManager
import services.streaming.base.{DefaultSourceDataProvider, StructuredZStream}
import services.streaming.throughput.base.ThroughputShaperBuilder
import services.synapse.versioning.SynapseWatermark
import services.synapse.versioning.SynapseWatermark.*

import com.sneaksanddata.arcane.framework.services.metrics.DeclaredMetrics
import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

class SynapseLinkDataProvider(
                               streamingSource: SynapseLinkStreamingSource,
                               sinkPropertyManager: SinkPropertyManager,
                               sinkSettings: SinkSettings,
                               throughputShaperBuilder: ThroughputShaperBuilder,
                               sourceBufferingSettings: SourceBufferingSettings,
                               declaredMetrics: DeclaredMetrics
) extends DefaultSourceDataProvider[SynapseWatermark](
  streamingSource,
      sinkPropertyManager,
      sinkSettings,
      throughputShaperBuilder,
      sourceBufferingSettings,
    declaredMetrics
    ):

  override def hasChanges(previousVersion: SynapseWatermark): Task[Boolean] =
    streamingSource.hasChanges(previousVersion)

  override def getCurrentVersion(previousVersion: SynapseWatermark): Task[SynapseWatermark] =
    streamingSource.getCurrentVersion(previousVersion)

  /** Implements data streaming logic for public `requestChanges`
    *
    * @param previousVersion
    *   Previous watermark
    * @return
    */
  override protected def changeStream(previousVersion: SynapseWatermark): ZStream[Any, Throwable, StructuredZStream] =
    streamingSource.getChanges(previousVersion)

object SynapseLinkDataProvider:
  type Environment = SynapseLinkStreamingSource & SinkPropertyManager & PluginStreamContext & ThroughputShaperBuilder & DeclaredMetrics

  val layer: ZLayer[Environment, Throwable, SynapseLinkDataProvider] = ZLayer {
    for
      context         <- ZIO.service[PluginStreamContext]
      propertyManager <- ZIO.service[SinkPropertyManager]
      streamingSource   <- ZIO.service[SynapseLinkStreamingSource]
      shaperBuilder   <- ZIO.service[ThroughputShaperBuilder]
      metrics <- ZIO.service[DeclaredMetrics]
    yield SynapseLinkDataProvider(
      streamingSource,
      propertyManager,
      context.sink,
      shaperBuilder,
      context.source.buffering,
      metrics
    )
  }
