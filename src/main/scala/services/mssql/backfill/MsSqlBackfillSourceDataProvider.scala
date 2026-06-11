package com.sneaksanddata.arcane.framework
package services.mssql.backfill

import models.app.PluginStreamContext
import models.settings.backfill.BackfillSettings
import models.settings.sources.SourceBufferingSettings
import models.sharding.{BootstrappedShard, DefaultBootstrappedShard}
import services.backfill.{DefaultBackfillSourceDataProvider, DefaultBackfillStateManager}
import services.mssql.base.MsSqlStreamingSource
import services.mssql.versioning.MsSqlWatermark
import services.naming.NameGenerator
import services.streaming.throughput.base.ThroughputShaperBuilder

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.OffsetDateTime

/** Backfill source data provider for Sql Server
  */
final class MsSqlBackfillSourceDataProvider(
    dataProvider: MsSqlStreamingSource,
    backfillSettings: BackfillSettings,
    stateManager: DefaultBackfillStateManager,
    throughputShaperBuilder: ThroughputShaperBuilder,
    sourceBufferingSettings: SourceBufferingSettings,
    nameGenerator: NameGenerator,
    backfillId: String
) extends DefaultBackfillSourceDataProvider[MsSqlWatermark](
      dataProvider,
      backfillSettings,
      throughputShaperBuilder,
      sourceBufferingSettings,
      stateManager
    ):

  override protected def backfillStream(
      backfillStart: MsSqlWatermark,
      backfillEnd: MsSqlWatermark,
      shardSources: Option[Seq[String]]
  ): ZStream[Any, Throwable, BootstrappedShard] = (shardSources match
    case None          => dataProvider.getShards(backfillStart, backfillEnd)
    case Some(sources) => ZStream.fromIterable(sources)
  )
    .mapZIO { preparedShardTableName =>
      for
        summaries         <- dataProvider.getColumnSummaries
        shardStream       <- dataProvider.createShardStream(preparedShardTableName, summaries)
        prefix            <- nameGenerator.getBackfillTablesPrefix
        backfillTableName <- nameGenerator.getBackfillTableName
        targetName        <- nameGenerator.getTargetTableFullName
      yield DefaultBootstrappedShard(
        shardStream = shardStream,
        shardSourceEntityName = preparedShardTableName,
        combinedTableName = backfillTableName,
        targetTableName = targetName,
        backfillId = backfillId
      )
    }

  override def getBackfillStartWatermark(startTime: Option[OffsetDateTime]): Task[MsSqlWatermark] = for wm <-
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
      dataProvider  <- ZIO.service[MsSqlStreamingSource]
      stateManager  <- ZIO.service[DefaultBackfillStateManager]
      context       <- ZIO.service[PluginStreamContext]
      shaper        <- ZIO.service[ThroughputShaperBuilder]
      nameGenerator <- ZIO.service[NameGenerator]
      backfillId    <- context.backfillId
    yield new MsSqlBackfillSourceDataProvider(
      dataProvider = dataProvider,
      backfillSettings = context.streamMode.backfill,
      stateManager = stateManager,
      backfillId = backfillId,
      throughputShaperBuilder = shaper,
      sourceBufferingSettings = context.source.buffering,
      nameGenerator = nameGenerator
    )
  }
