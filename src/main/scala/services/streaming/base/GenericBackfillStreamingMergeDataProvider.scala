package com.sneaksanddata.arcane.framework
package services.streaming.base

import logging.ZIOLogAnnotations.zlog
import services.app.base.StreamLifetimeService
import services.metrics.base.MetricTagProvider
import services.streaming.base.{BackfillOverwriteBatchFactory, BackfillStreamingMergeDataProvider, BackfillSubStream}

import zio.stream.ZPipeline
import zio.{Task, ZIO, ZIOAspect, ZLayer}

import scala.collection.SortedMap

/** Provides the backfill data stream for the streaming process. This data provider is used when backfill started with
  * the `Merge` behavior.
  */
class GenericBackfillStreamingMergeDataProvider(
    streamingGraphBuilder: BackfillSubStream,
    lifetimeService: StreamLifetimeService,
    tagProvider: MetricTagProvider
) extends BackfillStreamingMergeDataProvider:

  /** @inheritdoc
    */
  def requestBackfill: Task[Unit] =
    ZIO.attempt(tagProvider.getTags).flatMap { tags =>
      (for
        _ <- zlog("Starting backfill process")
        _ <- streamingGraphBuilder.produce().via(streamLifetimeGuard).runDrain
        _ <- zlog("Backfill process completed")
      yield ()) @@ ZIOAspect.tagged(Option(tags).getOrElse(SortedMap.empty[String, String]).toList*)
    }

  private def streamLifetimeGuard = ZPipeline.takeUntil(_ => lifetimeService.cancelled)

/** The companion object for the GenericBackfillStreamingMergeDataProvider class.
  */
object GenericBackfillStreamingMergeDataProvider:

  /** The environment required for the GenericBackfillStreamingMergeDataProvider.
    */
  type Environment = BackfillSubStream & StreamLifetimeService & BackfillOverwriteBatchFactory & MetricTagProvider

  /** Creates a new GenericBackfillStreamingMergeDataProvider.
    * @return
    *   The GenericBackfillStreamingMergeDataProvider instance.
    */
  def apply(
      streamingGraphBuilder: BackfillSubStream,
      lifetimeService: StreamLifetimeService,
      metricTagProvider: MetricTagProvider
  ): GenericBackfillStreamingMergeDataProvider =
    new GenericBackfillStreamingMergeDataProvider(
      streamingGraphBuilder,
      lifetimeService,
      metricTagProvider
    )

  /** The ZLayer for the GenericBackfillStreamingMergeDataProvider.
    */
  val layer: ZLayer[Environment, Nothing, GenericBackfillStreamingMergeDataProvider] =
    ZLayer.scoped {
      for
        streamingGraphBuilder <- ZIO.service[BackfillSubStream]
        lifetimeService       <- ZIO.service[StreamLifetimeService]
        metricTagProvider     <- ZIO.service[MetricTagProvider]
      yield GenericBackfillStreamingMergeDataProvider(
        streamingGraphBuilder,
        lifetimeService,
        metricTagProvider
      )
    }
