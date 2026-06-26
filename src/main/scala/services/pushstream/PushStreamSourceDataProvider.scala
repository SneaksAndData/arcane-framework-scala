package com.sneaksanddata.arcane.framework
package services.pushstream

import models.app.PluginStreamContext
import models.settings.sink.SinkSettings
import models.settings.sources.SourceBufferingSettings
import services.iceberg.base.SinkPropertyManager
import services.pushstream.versioning.PushStreamWatermark
import services.streaming.base.{DefaultSourceDataProvider, StructuredZStream}
import services.streaming.throughput.base.ThroughputShaperBuilder

import zio.{Task, ZIO, ZLayer}
import zio.stream.ZStream

class PushStreamSourceDataProvider(
    source: PushStreamingSource,
    sinkPropertyManager: SinkPropertyManager,
    sinkSettings: SinkSettings,
    throughputShaperBuilder: ThroughputShaperBuilder,
    sourceBufferingSettings: SourceBufferingSettings
) extends DefaultSourceDataProvider[PushStreamWatermark](
      sinkPropertyManager,
      sinkSettings,
      throughputShaperBuilder,
      sourceBufferingSettings
    ):
  override protected def changeStream(
      previousVersion: PushStreamWatermark
  ): ZStream[Any, Throwable, StructuredZStream] =
    source.getChanges(previousVersion)

  /** Checks whether the provided watermark from previous iteration has accrued any changes in [previousVersion ... now]
    * interval
    *
    * @param previousVersion
    *   Watermark from the previous change capture iteration
    * @return
    */
  override def hasChanges(previousVersion: PushStreamWatermark): Task[Boolean] = source.hasRows(previousVersion)

  /** Most recent version of a source dataset, compared. This should return previousVersion in case retrieval of a most
    * recent version failed.
    *
    * @param previousVersion
    *   Watermark from the previous change capture iteration
    * @return
    */
  override def getCurrentVersion(previousVersion: PushStreamWatermark): Task[PushStreamWatermark] =
    source.getMaxTimestamp

object PushStreamSourceDataProvider:
  private type Environment =
    PushStreamingSource & SinkPropertyManager & PluginStreamContext & ThroughputShaperBuilder

  val layer: ZLayer[Environment, Nothing, PushStreamSourceDataProvider] = ZLayer {
    for
      context           <- ZIO.service[PluginStreamContext]
      source            <- ZIO.service[PushStreamingSource]
      propertyManager   <- ZIO.service[SinkPropertyManager]
      throughputBuilder <- ZIO.service[ThroughputShaperBuilder]
    yield new PushStreamSourceDataProvider(
      source,
      propertyManager,
      context.sink,
      throughputBuilder,
      context.source.buffering
    )
  }

