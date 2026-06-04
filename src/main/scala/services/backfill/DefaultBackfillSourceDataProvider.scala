package com.sneaksanddata.arcane.framework
package services.backfill

import extensions.ZExtensions.trySetBuffering
import logging.ZIOLogAnnotations.zlogStream
import models.backfill.DefaultSourceBackfill
import models.settings.backfill.BackfillSettings
import models.settings.sources.SourceBufferingSettings
import models.sharding.{BootstrappedShard, DefaultBootstrappedShard}
import services.backfill.base.BackfillSourceDataProvider
import services.base.StreamingSource
import services.streaming.base.*
import services.streaming.throughput.base.ThroughputShaperBuilder

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
    dataProvider: StreamingSource,
    backfillSettings: BackfillSettings,
    throughputShaperBuilder: ThroughputShaperBuilder,
    sourceBufferingSettings: SourceBufferingSettings,
    stateManager: DefaultBackfillStateManager
)(implicit rw: ReadWriter[WatermarkType])
    extends BackfillSourceDataProvider[WatermarkType]:

  private val throughputShaper = throughputShaperBuilder.build

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
          .flatMap(v => zlogStream("Prepared total %s shards for the backfill", v.size.toString) *> ZStream.succeed(v))
          .flatMap { bootstrapped =>
            val outputStream = ZStream.fromIterable(bootstrapped.map { case unshaped: DefaultBootstrappedShard =>
              unshaped.copy(
                shardStream = (
                  throughputShaper.shapeStream(unshaped.shardStream._1.trySetBuffering(sourceBufferingSettings)),
                  unshaped.shardStream._2
                )
              )
            })
            if shards.isDefined then outputStream
            else {
              val backfillMetadata = DefaultSourceBackfill(
                bootstrapped.head.backfillId,
                startFrom.toJson,
                snapshotVersion.toJson,
                bootstrapped.map(_.shardSourceEntityName)
              )
              ZStream.fromZIO(stateManager.commitState(backfillMetadata)).flatMap(_ => outputStream)
            }
          }
      }
