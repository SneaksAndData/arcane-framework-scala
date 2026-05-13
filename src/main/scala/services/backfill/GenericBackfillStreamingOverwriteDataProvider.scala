package com.sneaksanddata.arcane.framework
package services.backfill

import logging.ZIOLogAnnotations.zlog
import models.app.PluginStreamContext
import models.batches.{MergeableBatch, StagedVersionedBatch}
import models.schemas.ArcaneSchema
import models.settings.staging.StagingTableSettings
import services.app.base.StreamLifetimeService
import services.backfill.{BackfillOverwriteBatchFactory, BackfillStreamingOverwriteDataProvider, BackfillSubStream}
import services.metrics.base.MetricTagProvider
import services.streaming.base.*
import services.streaming.processors.transformers.StagingProcessor

import org.apache.iceberg.Table
import zio.stream.ZPipeline
import zio.{Chunk, Task, ZIO, ZIOAspect, ZLayer}

import scala.collection.SortedMap

/** Provides the backfill data stream for the streaming process. It is utilized when the backfill process begins with
  * the `overwrite` behavior. An important distinction between this and the GenericBackfillStreamingMergeDataProvider is
  * that this provider overrides the table used by the basic streamGraphBuilder, replacing it with the intermediate
  * backfill table. Additionally, this data provider can generate a backfill batch as a result of the backfill process,
  * or it may produce nothing if the backfill was interrupted.
  */
class GenericBackfillStreamingOverwriteDataProvider(
    streamingGraphBuilder: BackfillSubStream,
    stagingTableSettings: StagingTableSettings,
    lifetimeService: StreamLifetimeService,
    backfillBatchFactory: BackfillOverwriteBatchFactory,
    metricTagProvider: MetricTagProvider
) extends BackfillStreamingOverwriteDataProvider:

  /** @inheritdoc
    */
  def requestBackfill: Task[BatchType] =
    ZIO.attempt(metricTagProvider.getTags).flatMap { tags =>
      (for
        _ <- zlog("Starting backfill process")
        lastBatch <- streamingGraphBuilder
          .produce()
          .via(streamLifetimeGuard)
          .runLast // ensure watermark is emitted at the end
        _ <- zlog("Backfill process completed")

        backfillBatch <-
          if lifetimeService.cancelled then ZIO.unit
          else
            backfillBatchFactory.createBackfillBatch(
              lastBatch.flatMap(_.completedWatermarkValue)
            )
      yield backfillBatch) @@ ZIOAspect.tagged(Option(tags).getOrElse(SortedMap.empty[String, String]).toList*)
    }

  private def streamLifetimeGuard =
    ZPipeline[BackfillSubStream#ProcessedBatch].takeUntil(_ => lifetimeService.cancelled)

/** The companion object for the GenericBackfillStreamingOverwriteDataProvider class.
  */
object GenericBackfillStreamingOverwriteDataProvider:

  /** The environment required for the GenericBackfillStreamingOverwriteDataProvider.
    */
  type Environment = BackfillSubStream & PluginStreamContext & StreamLifetimeService & BackfillOverwriteBatchFactory &
    MetricTagProvider

  /** Creates a new GenericBackfillStreamingOverwriteDataProvider.
    * @param streamingGraphBuilder
    *   The streaming graph builder.
    * @param lifetimeService
    *   The stream lifetime service.
    * @return
    *   The GenericBackfillStreamingOverwriteDataProvider instance.
    */
  def apply(
      streamingGraphBuilder: BackfillSubStream,
      stagingTableSettings: StagingTableSettings,
      lifetimeService: StreamLifetimeService,
      backfillBatchFactory: BackfillOverwriteBatchFactory,
      metricTagProvider: MetricTagProvider
  ): GenericBackfillStreamingOverwriteDataProvider =
    new GenericBackfillStreamingOverwriteDataProvider(
      streamingGraphBuilder,
      stagingTableSettings,
      lifetimeService,
      backfillBatchFactory,
      metricTagProvider
    )

  /** The ZLayer for the GenericBackfillStreamingOverwriteDataProvider.
    */
  val layer: ZLayer[Environment, Nothing, BackfillStreamingOverwriteDataProvider] =
    ZLayer {
      for
        context               <- ZIO.service[PluginStreamContext]
        streamingGraphBuilder <- ZIO.service[BackfillSubStream]
        lifetimeService       <- ZIO.service[StreamLifetimeService]
        backfillBatchFactory  <- ZIO.service[BackfillOverwriteBatchFactory]
        metricTagProvider     <- ZIO.service[MetricTagProvider]
      yield GenericBackfillStreamingOverwriteDataProvider(
        streamingGraphBuilder,
        context.staging.table,
        lifetimeService,
        backfillBatchFactory,
        metricTagProvider
      )
    }
