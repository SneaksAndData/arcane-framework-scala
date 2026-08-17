package com.sneaksanddata.arcane.framework
package services.blobsource.providers

import models.app.PluginStreamContext
import models.settings.sink.SinkSettings
import models.settings.sources.SourceBufferingSettings
import services.blobsource.readers.BlobStreamingSource
import services.blobsource.versioning.BlobSourceWatermark
import services.blobsource.versioning.BlobSourceWatermark.*
import services.iceberg.base.SinkPropertyManager
import services.metrics.DeclaredMetrics
import services.streaming.base.{DefaultSourceDataProvider, StructuredZStream}
import services.streaming.throughput.base.ThroughputShaperBuilder

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.OffsetDateTime

class BlobSourceDataProvider(
    streamingSource: BlobStreamingSource,
    sinkPropertyManager: SinkPropertyManager,
    sinkSettings: SinkSettings,
    throughputShaperBuilder: ThroughputShaperBuilder,
    sourceBufferingSettings: SourceBufferingSettings,
    declaredMetrics: DeclaredMetrics
) extends DefaultSourceDataProvider[BlobSourceWatermark](
      streamingSource,
      sinkPropertyManager,
      sinkSettings,
      throughputShaperBuilder,
      sourceBufferingSettings,
      declaredMetrics
    ):

  override def hasChanges(previousVersion: BlobSourceWatermark): Task[Boolean] =
    streamingSource.hasChanges(previousVersion)

  override def getCurrentVersion(previousVersion: BlobSourceWatermark): Task[BlobSourceWatermark] =
    streamingSource.getLatestVersion

  override protected def changeStream(
      previousVersion: BlobSourceWatermark
  ): ZStream[Any, Throwable, StructuredZStream] =
    streamingSource.getChanges(previousVersion)

  override def resolveWatermark(timestamp: OffsetDateTime): Task[BlobSourceWatermark] =
    streamingSource.resolveWatermark(timestamp)

object BlobSourceDataProvider:
  private type Environment = BlobStreamingSource & SinkPropertyManager & PluginStreamContext & ThroughputShaperBuilder &
    DeclaredMetrics

  val layer: ZLayer[Environment, Throwable, BlobSourceDataProvider] = ZLayer {
    for
      context           <- ZIO.service[PluginStreamContext]
      propertyManager   <- ZIO.service[SinkPropertyManager]
      blobSource        <- ZIO.service[BlobStreamingSource]
      throughputBuilder <- ZIO.service[ThroughputShaperBuilder]
      metrics           <- ZIO.service[DeclaredMetrics]
    yield BlobSourceDataProvider(
      blobSource,
      propertyManager,
      context.sink,
      throughputBuilder,
      context.source.buffering,
      metrics
    )
  }
