package com.sneaksanddata.arcane.framework
package services.synapse.backfill

import models.app.PluginStreamContext
import models.settings.backfill.BackfillSettings
import models.settings.sink.SinkSettings
import models.settings.sources.SourceBufferingSettings
import models.sharding.{BootstrappedShard, DefaultBootstrappedShard}
import services.backfill.{DefaultBackfillSourceDataProvider, DefaultBackfillStateManager}
import services.naming.NameGenerator
import services.streaming.throughput.base.ThroughputShaperBuilder
import services.synapse.base.SynapseLinkStreamingSource
import services.synapse.versioning.SynapseWatermark

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.OffsetDateTime

/** Backfill source data provider for Synapse Link
  */
final class SynapseBackfillSourceDataProvider(
                                               dataProvider: SynapseLinkStreamingSource,
                                               backfillSettings: BackfillSettings,
                                               stateManager: DefaultBackfillStateManager,
                                               throughputShaperBuilder: ThroughputShaperBuilder,
                                               sourceBufferingSettings: SourceBufferingSettings,
                                               nameGenerator: NameGenerator,
                                               backfillId: String
) extends DefaultBackfillSourceDataProvider[SynapseWatermark](
      dataProvider,
      backfillSettings,
      throughputShaperBuilder,
      sourceBufferingSettings,
      stateManager
    ):

  override protected def backfillStream(
      backfillStart: SynapseWatermark,
      backfillEnd: SynapseWatermark,
      shardSources: Option[Seq[String]]
  ): ZStream[Any, Throwable, BootstrappedShard] = (shardSources match
    case None =>
      dataProvider.getShards(backfillId, backfillStart, backfillEnd)
    case Some(sources) => ZStream.fromIterable(sources).mapZIO(dataProvider.getShardFolderStream).collect {
      case Some(streamMetadata) => streamMetadata
    }
  )
    .mapZIO { case (stream, source) =>
      for
        backfillTableName <- nameGenerator.getBackfillTableName
        prefix            <- nameGenerator.getBackfillTablesPrefix
        targetName        <- nameGenerator.getTargetTableFullName
      yield DefaultBootstrappedShard(
        shardStream = stream,
        shardSourceEntityName = source,
        combinedTableName = backfillTableName,
        targetTableName = targetName,
        backfillId = backfillId
      )
    }

  override protected def getBackfillStartWatermark(startTime: Option[OffsetDateTime]): Task[SynapseWatermark] = for
    _  <- ZIO.when(startTime.isEmpty)(ZIO.fail(new IllegalArgumentException("Backfill start date is not set")))
    wm <- dataProvider.getWatermark(startTime.get)
  yield wm

  /** Most recent version of the dataset at a time when a backfill was initiated.
    */
  override def getSnapshotVersion: Task[SynapseWatermark] = dataProvider.getCurrentVersion(SynapseWatermark.epoch)

object SynapseBackfillSourceDataProvider:
  val layer = ZLayer {
    for
      dataProvider  <- ZIO.service[SynapseLinkStreamingSource]
      stateManager  <- ZIO.service[DefaultBackfillStateManager]
      context       <- ZIO.service[PluginStreamContext]
      shaper        <- ZIO.service[ThroughputShaperBuilder]
      nameGenerator <- ZIO.service[NameGenerator]
      backfillId    <- context.backfillId
    yield new SynapseBackfillSourceDataProvider(
      dataProvider = dataProvider,
      backfillSettings = context.streamMode.backfill,
      stateManager = stateManager,
      backfillId = backfillId,
      throughputShaperBuilder = shaper,
      sourceBufferingSettings = context.source.buffering,
      nameGenerator = nameGenerator
    )
  }
