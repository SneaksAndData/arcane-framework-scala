package com.sneaksanddata.arcane.framework
package services.mssql.backfill

import models.app.PluginStreamContext
import models.settings.TableNaming.getBackfillTableName
import models.settings.backfill.BackfillSettings
import models.settings.sink.SinkSettings
import models.settings.sources.SourceBufferingSettings
import models.sharding.{BootstrappedShard, DefaultBootstrappedShard}
import services.backfill.{DefaultBackfillSourceDataProvider, DefaultBackfillStateManager}
import services.streaming.throughput.base.ThroughputShaperBuilder

import com.sneaksanddata.arcane.framework.services.mssql.base.MsSqlReader
import com.sneaksanddata.arcane.framework.services.mssql.versioning.MsSqlWatermark
import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.OffsetDateTime

/** Backfill source data provider for Sql Server
  */
final class MsSqlBackfillSourceDataProvider(
    dataProvider: MsSqlReader,
    backfillSettings: BackfillSettings,
    sinkSettings: SinkSettings,
    stateManager: DefaultBackfillStateManager,
    throughputShaperBuilder: ThroughputShaperBuilder,
    sourceBufferingSettings: SourceBufferingSettings,
    backfillId: String
) extends DefaultBackfillSourceDataProvider[MsSqlWatermark](
      dataProvider,
      backfillSettings,
      sinkSettings,
      throughputShaperBuilder,
      sourceBufferingSettings,
      stateManager
    ):

  // TODO: backfill-merge should not shard but rather run a single load. NYI
  override protected def backfillStream(
      backfillStart: MsSqlWatermark,
      backfillEnd: MsSqlWatermark,
      shardSources: Option[Seq[String]]
  ): ZStream[Any, Throwable, BootstrappedShard] = (shardSources match
    case None          => dataProvider.prepareShardTables(backfillId, None)
    case Some(sources) => ZStream.fromIterable(sources)
  )
    .mapZIO { preparedShardTableName =>
      for shardStream <- dataProvider.createShardStream(preparedShardTableName)
      yield DefaultBootstrappedShard(
        shardStream = shardStream,
        shardSourceEntityName = preparedShardTableName,
        combinedTableName = getBackfillTableName(backfillId),
        targetTableName = sinkSettings.targetTableFullName,
        backfillId = backfillId
      )
    }

  override protected def getBackfillStartWatermark(startTime: Option[OffsetDateTime]): Task[MsSqlWatermark] = for wm <-
      ZIO
        .attempt(startTime match
          case Some(start) => dataProvider.timestampToVersion(start)
          case None        => ZIO.succeed(MsSqlWatermark.epoch))
        .flatten
  yield wm

  /** Most recent version of the dataset at a time when a backfill was initiated.
    */
  override def getSnapshotVersion: Task[MsSqlWatermark] = dataProvider.getCurrentVersion

object MsSqlBackfillSourceDataProvider:
  val layer = ZLayer {
    for
      dataProvider <- ZIO.service[MsSqlReader]
      stateManager <- ZIO.service[DefaultBackfillStateManager]
      context      <- ZIO.service[PluginStreamContext]
      shaper       <- ZIO.service[ThroughputShaperBuilder]
      backfillId   <- context.backfillId
    yield new MsSqlBackfillSourceDataProvider(
      dataProvider = dataProvider,
      backfillSettings = context.streamMode.backfill,
      sinkSettings = context.sink,
      stateManager = stateManager,
      backfillId = backfillId,
      throughputShaperBuilder = shaper,
      sourceBufferingSettings = context.source.buffering
    )
  }
