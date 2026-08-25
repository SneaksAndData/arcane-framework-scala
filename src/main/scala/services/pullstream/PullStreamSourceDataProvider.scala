package com.sneaksanddata.arcane.framework
package services.pullstream

import models.app.PluginStreamContext
import models.settings.sink.SinkSettings
import models.settings.sources.SourceBufferingSettings
import services.iceberg.base.SinkPropertyManager
import services.metrics.DeclaredMetrics
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
    sourceBufferingSettings: SourceBufferingSettings,
    declaredMetrics: DeclaredMetrics
) extends DefaultSourceDataProvider[PullStreamWatermark](
      source,
      sinkPropertyManager,
      sinkSettings,
      throughputShaperBuilder,
      sourceBufferingSettings,
      declaredMetrics
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

  /** The pull stream has no notion of discrete versions between two watermarks: the source is queried by timestamp and
    * every row in the interval is read, so there is nothing to cut the range at and the end watermark is always used.
    */
  override def getLatestWatermarkInRange(
      startWatermark: PullStreamWatermark,
      endWatermark: PullStreamWatermark,
      rangeLimit: Int
  ): Task[PullStreamWatermark] = ZIO.succeed(endWatermark)

object PullStreamSourceDataProvider:
  private type Environment =
    PullStreamingSource & SinkPropertyManager & PluginStreamContext & ThroughputShaperBuilder & DeclaredMetrics

  val layer: ZLayer[Environment, Nothing, PullStreamSourceDataProvider] = ZLayer {
    for
      context           <- ZIO.service[PluginStreamContext]
      source            <- ZIO.service[PullStreamingSource]
      propertyManager   <- ZIO.service[SinkPropertyManager]
      throughputBuilder <- ZIO.service[ThroughputShaperBuilder]
      declaredMetrics   <- ZIO.service[DeclaredMetrics]
    yield new PullStreamSourceDataProvider(
      source,
      propertyManager,
      context.sink,
      throughputBuilder,
      context.source.buffering,
      declaredMetrics
    )
  }
