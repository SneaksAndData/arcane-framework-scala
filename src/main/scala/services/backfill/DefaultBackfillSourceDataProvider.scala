package com.sneaksanddata.arcane.framework
package services.backfill

import models.backfill.DefaultSourceBackfill
import models.schemas.ArcaneSchema
import models.settings.backfill.BackfillSettings
import models.settings.sink.SinkSettings
import models.sharding.BootstrappedShard
import services.backfill.base.BackfillSourceDataProvider
import services.base.SchemaProvider
import services.streaming.base.*

import upickle.ReadWriter
import zio.Task
import zio.stream.{ZPipeline, ZSink, ZStream}

import java.time.OffsetDateTime

/** Provides the backfill data stream for the streaming process. It is utilized when the backfill process begins with
  * the `overwrite` behavior. An important distinction between this and the GenericBackfillStreamingMergeDataProvider is
  * that this provider overrides the table used by the basic streamGraphBuilder, replacing it with the intermediate
  * backfill table. Additionally, this data provider can generate a backfill batch as a result of the backfill process,
  * or it may produce nothing if the backfill was interrupted.
  */
abstract class DefaultBackfillSourceDataProvider[WatermarkType <: SourceWatermark[String] & JsonWatermark](
    dataProvider: SchemaProvider[ArcaneSchema],
    backfillSettings: BackfillSettings,
    sinkSettings: SinkSettings,
    stateManager: DefaultBackfillStateManager
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
      backfillEnd: WatermarkType,
      shardSources: Option[Seq[String]]
  ): ZStream[Any, Throwable, BootstrappedShard]

  private def collectShards = ZPipeline.fromSink(ZSink.collectAll[BootstrappedShard])

  final override def requestBackfill(
      snapshotVersion: WatermarkType,
      shards: Option[Seq[String]]
  ): ZStream[Any, Throwable, BootstrappedShard] =
    ZStream
      .fromZIO(getBackfillStartWatermark(backfillSettings.backfillStartDate))
      .flatMap { startFrom =>
        backfillStream(startFrom, snapshotVersion, shards)
          .via(collectShards)
          .flatMap { bootstrapped =>
            val outputStream = ZStream.fromIterable(bootstrapped)
            if shards.isDefined then outputStream
            else {
              val backfillMetadata = DefaultSourceBackfill(
                "",
                startFrom.toJson,
                snapshotVersion.toJson,
                "",
                bootstrapped.map(_.shardSourceEntityName)
              )
              ZStream.fromZIO(stateManager.commitState(backfillMetadata)).flatMap(_ => outputStream)
            }
          }
      }
