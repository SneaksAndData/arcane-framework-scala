package com.sneaksanddata.arcane.framework
package services.blobsource.providers

import models.app.PluginStreamContext
import models.settings.sink.SinkSettings
import models.settings.sources.SourceBufferingSettings
import services.blobsource.readers.BlobSourceReader
import services.blobsource.versioning.BlobSourceWatermark
import services.blobsource.versioning.BlobSourceWatermark.*
import services.iceberg.base.SinkPropertyManager
import services.streaming.base.{DefaultSourceDataProvider, StructuredZStream}
import services.streaming.throughput.base.ThroughputShaperBuilder

import com.sneaksanddata.arcane.framework.services.metrics.DeclaredMetrics
import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

class BlobSourceDataProvider(
    sourceReader: BlobSourceReader,
    sinkPropertyManager: SinkPropertyManager,
    sinkSettings: SinkSettings,
    throughputShaperBuilder: ThroughputShaperBuilder,
    sourceBufferingSettings: SourceBufferingSettings,
    declaredMetrics: DeclaredMetrics
) extends DefaultSourceDataProvider[BlobSourceWatermark](
      sourceReader,
      sinkPropertyManager,
      sinkSettings,
      throughputShaperBuilder,
      sourceBufferingSettings,
      declaredMetrics
    ):

  override def hasChanges(previousVersion: BlobSourceWatermark): Task[Boolean] =
    sourceReader.hasChanges(previousVersion)

  override def getCurrentVersion(previousVersion: BlobSourceWatermark): Task[BlobSourceWatermark] =
    sourceReader.getLatestVersion

  override protected def changeStream(
      previousVersion: BlobSourceWatermark
  ): ZStream[Any, Throwable, StructuredZStream] =
    sourceReader.getChanges(previousVersion)

object BlobSourceDataProvider:
  private type Environment = BlobSourceReader & SinkPropertyManager & PluginStreamContext & ThroughputShaperBuilder & DeclaredMetrics

  val layer: ZLayer[Environment, Throwable, BlobSourceDataProvider] = ZLayer {
    for
      context           <- ZIO.service[PluginStreamContext]
      propertyManager   <- ZIO.service[SinkPropertyManager]
      blobSource        <- ZIO.service[BlobSourceReader]
      throughputBuilder <- ZIO.service[ThroughputShaperBuilder]
      metrics <- ZIO.service[DeclaredMetrics]
    yield BlobSourceDataProvider(
      blobSource,
      propertyManager,
      context.sink,
      throughputBuilder,
      context.source.buffering,
      metrics
    )
  }
