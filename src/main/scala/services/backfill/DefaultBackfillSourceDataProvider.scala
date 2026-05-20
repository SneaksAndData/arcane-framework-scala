package com.sneaksanddata.arcane.framework
package services.backfill

import models.settings.backfill.BackfillSettings
import models.settings.sink.SinkSettings
import models.settings.staging.StagingTableSettings
import models.sharding.BootstrappedShard
import services.metrics.base.MetricTagProvider
import services.streaming.base.*

import com.sneaksanddata.arcane.framework.services.backfill.base.BackfillSourceDataProvider
import upickle.ReadWriter
import zio.Task
import zio.stream.ZStream

import java.time.OffsetDateTime

/** Provides the backfill data stream for the streaming process. It is utilized when the backfill process begins with
  * the `overwrite` behavior. An important distinction between this and the GenericBackfillStreamingMergeDataProvider is
  * that this provider overrides the table used by the basic streamGraphBuilder, replacing it with the intermediate
  * backfill table. Additionally, this data provider can generate a backfill batch as a result of the backfill process,
  * or it may produce nothing if the backfill was interrupted.
  */
abstract class DefaultBackfillSourceDataProvider[WatermarkType <: SourceWatermark[String] & JsonWatermark](
    dataProvider: BackfillSourceDataProvider[WatermarkType],
    backfillSettings: BackfillSettings,
    stagingTableSettings: StagingTableSettings,
    sinkSettings: SinkSettings,
    metricTagProvider: MetricTagProvider
)(implicit rw: ReadWriter[WatermarkType])
    extends BackfillSourceDataProvider[WatermarkType]:

  /** Evaluates watermark to be used when evaluating current snapshot version at the start of a backfill process
    *
    * @return
    */
  protected def getBackfillStartWatermark(startTime: Option[OffsetDateTime]): Task[WatermarkType]

  /** Implements data streaming logic for public `requestBackfill`
    *
    * @return
    */
  protected def backfillStream(
      backfillStart: WatermarkType,
      shardCount: Int
  ): ZStream[Any, Throwable, BootstrappedShard]

  protected def hasData(backfillStart: WatermarkType): Task[Boolean]

  final override def isEmpty: Task[Boolean] =
    getBackfillStartWatermark(backfillSettings.backfillStartDate).flatMap(hasData)

  final override def requestBackfill: ZStream[Any, Throwable, BootstrappedShard] = ZStream
    .fromZIO(getSnapshotVersion)
    .mapZIO { snapshotWatermark =>
      for
        shards             <- getShardCount
        startFromWatermark <- getBackfillStartWatermark(backfillSettings.backfillStartDate)
      yield (snapshot = snapshotWatermark, startFrom = startFromWatermark, shardCount = shards)
    }
    .flatMap { case (snapshot, startFrom, shardCount) =>
      backfillStream(startFrom, shardCount)
    }

  /** @inheritdoc
    */
//  def requestBackfill: Task[BatchType] =
//    ZIO.attempt(metricTagProvider.getTags).flatMap { tags =>
//      (for
//        _ <- zlog("Starting backfill process")
//        lastBatch <- streamingGraphBuilder
//          .produce()
//          .via(streamLifetimeGuard)
//          .runLast // ensure watermark is emitted at the end
//        _ <- zlog("Backfill process completed")
//
//        backfillBatch <-
//          if lifetimeService.cancelled then ZIO.unit
//          else
//            backfillBatchFactory.createBackfillBatch(
//              lastBatch.flatMap(_.completedWatermarkValue)
//            )
//      yield backfillBatch) @@ ZIOAspect.tagged(Option(tags).getOrElse(SortedMap.empty[String, String]).toList*)
//    }
//
//  private def streamLifetimeGuard =
//    ZPipeline[BackfillSubStream#ProcessedBatch].takeUntil(_ => lifetimeService.cancelled)
//
///** The companion object for the GenericBackfillStreamingOverwriteDataProvider class.
//  */
//object DefaultBackfillStreamDataProvider:
//
//  /** The environment required for the GenericBackfillStreamingOverwriteDataProvider.
//    */
//  type Environment = PluginStreamContext & StreamLifetimeService & BackfillOverwriteBatchFactory &
//    MetricTagProvider

//  /** Creates a new GenericBackfillStreamingOverwriteDataProvider.
//    */
//  def apply(
//      stagingTableSettings: StagingTableSettings,
//      lifetimeService: StreamLifetimeService,
//      backfillBatchFactory: BackfillOverwriteBatchFactory,
//      metricTagProvider: MetricTagProvider
//  ): DefaultBackfillStreamDataProvider =
//    new DefaultBackfillStreamDataProvider(
//      stagingTableSettings,
//      lifetimeService,
//      backfillBatchFactory,
//      metricTagProvider
//    )
//
//  /** The ZLayer for the GenericBackfillStreamingOverwriteDataProvider.
//    */
//  val layer: ZLayer[Environment, Nothing, BackfillStreamDataProvider] =
//    ZLayer {
//      for
//        context               <- ZIO.service[PluginStreamContext]
//        lifetimeService       <- ZIO.service[StreamLifetimeService]
//        backfillBatchFactory  <- ZIO.service[BackfillOverwriteBatchFactory]
//        metricTagProvider     <- ZIO.service[MetricTagProvider]
//      yield DefaultBackfillStreamDataProvider(
//        context.staging.table,
//        lifetimeService,
//        backfillBatchFactory,
//        metricTagProvider
//      )
//    }
