package com.sneaksanddata.arcane.framework
package services.pullstream

import models.app.PluginStreamContext
import models.settings.sink.SinkSettings
import models.settings.sources.SourceBufferingSettings
import services.iceberg.base.SinkPropertyManager
import services.pullstream.versioning.PullStreamWatermark
import services.streaming.base.{DefaultSourceDataProvider, StructuredZStream}
import services.streaming.throughput.base.ThroughputShaperBuilder

import zio.{Task, ZIO, ZLayer}
import zio.stream.ZStream

class PullStreamSourceDataProvider(
    source: PullStreamingSource,
    sinkPropertyManager: SinkPropertyManager,
    sinkSettings: SinkSettings,
    throughputShaperBuilder: ThroughputShaperBuilder,
    sourceBufferingSettings: SourceBufferingSettings
) extends DefaultSourceDataProvider[PullStreamWatermark](
      sinkPropertyManager,
      sinkSettings,
      throughputShaperBuilder,
      sourceBufferingSettings
    ):
  override protected def changeStream(
      previousVersion: PullStreamWatermark
  ): ZStream[Any, Throwable, StructuredZStream] =
    source.getChanges(previousVersion)

  /** Checks whether the provided watermark from previous iteration has accrued any changes in [previousVersion ... now]
    * interval
    *
    * @param previousVersion
    *   Watermark from the previous change capture iteration
    * @return
    */
  override def hasChanges(previousVersion: PullStreamWatermark): Task[Boolean] = source.hasRows(previousVersion)

  /** Most recent version of a source dataset, compared. This should return previousVersion in case retrieval of a most
    * recent version failed.
    *
    * @param previousVersion
    *   Watermark from the previous change capture iteration
    * @return
    */
  override def getCurrentVersion(previousVersion: PullStreamWatermark): Task[PullStreamWatermark] =
    source.getMaxTimestamp

object PullStreamSourceDataProvider:
  private type Environment =
    PullStreamingSource & SinkPropertyManager & PluginStreamContext & ThroughputShaperBuilder

  val layer: ZLayer[Environment, Nothing, PullStreamSourceDataProvider] = ZLayer {
    for
      context           <- ZIO.service[PluginStreamContext]
      source            <- ZIO.service[PullStreamingSource]
      propertyManager   <- ZIO.service[SinkPropertyManager]
      throughputBuilder <- ZIO.service[ThroughputShaperBuilder]
    yield new PullStreamSourceDataProvider(
      source,
      propertyManager,
      context.sink,
      throughputBuilder,
      context.source.buffering
    )
  }
