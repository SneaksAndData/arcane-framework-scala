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

  /** Returns the end watermark unchanged, so the read range and the recorded watermark always advance together.
    *
    * `requestChanges` reads with `buildQueryGetChanges`, whose key condition is open-ended (`#wm > :t`) and therefore
    * always drains to the newest row present, while the watermark persisted for the next iteration is the value
    * returned here. Returning anything earlier than `endWatermark` would leave the watermark trailing the rows that
    * were already read, and the next iteration would read that same tail again: the overlap grows with the backlog, so
    * catching up costs a pass over the remaining backlog per shortened step rather than a single pass overall.
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
