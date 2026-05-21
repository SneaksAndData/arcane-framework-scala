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

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

class SynapseLinkDataProvider(
    synapseReader: SynapseLinkReader,
    sinkPropertyManager: SinkPropertyManager,
    sinkSettings: SinkSettings,
    throughputShaperBuilder: ThroughputShaperBuilder,
    sourceBufferingSettings: SourceBufferingSettings
) extends DefaultSourceDataProvider[SynapseWatermark](
      sinkPropertyManager,
      sinkSettings,
      throughputShaperBuilder,
      sourceBufferingSettings
    ):
  
  override def hasChanges(previousVersion: SynapseWatermark): Task[Boolean] =
    synapseReader.hasChanges(previousVersion)

  override def getCurrentVersion(previousVersion: SynapseWatermark): Task[SynapseWatermark] =
    synapseReader.getCurrentVersion(previousVersion)

  /** Implements data streaming logic for public `requestChanges`
    *
    * @param previousVersion
    *   Previous watermark
    * @return
    */
  override protected def changeStream(previousVersion: SynapseWatermark): ZStream[Any, Throwable, StructuredZStream] =
    synapseReader.getChanges(previousVersion)

object SynapseLinkDataProvider:
  type Environment = SynapseLinkReader & SinkPropertyManager & PluginStreamContext & ThroughputShaperBuilder

  val layer: ZLayer[Environment, Throwable, SynapseLinkDataProvider] = ZLayer {
    for
      context         <- ZIO.service[PluginStreamContext]
      propertyManager <- ZIO.service[SinkPropertyManager]
      synapseReader   <- ZIO.service[SynapseLinkReader]
      shaperBuilder   <- ZIO.service[ThroughputShaperBuilder]
    yield SynapseLinkDataProvider(
      synapseReader,
      propertyManager,
      context.sink,
      shaperBuilder,
      context.source.buffering
    )
  }
