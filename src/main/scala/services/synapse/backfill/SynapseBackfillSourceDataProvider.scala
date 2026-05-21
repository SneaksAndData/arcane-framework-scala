package com.sneaksanddata.arcane.framework
package services.synapse.backfill

import models.settings.backfill.BackfillSettings
import models.settings.sink.SinkSettings
import models.settings.staging.StagingTableSettings
import models.sharding.{BootstrappedShard, DefaultBootstrappedShard}
import services.backfill.DefaultBackfillSourceDataProvider
import services.metrics.base.MetricTagProvider
import services.synapse.base.SynapseLinkReader
import services.synapse.versioning.SynapseWatermark

import zio.stream.ZStream
import zio.{Task, ZIO}

import java.time.OffsetDateTime

/** Backfill source data provider for Synapse Link
  */
final class SynapseBackfillSourceDataProvider(
    dataProvider: SynapseLinkReader,
    backfillSettings: BackfillSettings,
    stagingTableSettings: StagingTableSettings,
    sinkSettings: SinkSettings,
    metricTagProvider: MetricTagProvider
) extends DefaultBackfillSourceDataProvider[SynapseWatermark](
      dataProvider,
      backfillSettings,
      stagingTableSettings,
      sinkSettings,
      metricTagProvider
    ):

  override protected def backfillStream(
      backfillStart: SynapseWatermark,
      backfillEnd: SynapseWatermark,
      shardSources: Option[Seq[String]]
  ): ZStream[Any, Throwable, BootstrappedShard] = (shardSources match
    case None => dataProvider
      .getData(backfillStart, backfillEnd)
    case Some(sources) => dataProvider.getData(sources))
    .map { case (stream, source) =>
      DefaultBootstrappedShard(
        shardStream = stream,
        shardSourceEntityName = source,
        combinedTableName = stagingTableSettings.backfillTableName,
        targetTableName = sinkSettings.targetTableFullName
      )
    }
    

  override protected def getBackfillStartWatermark(startTime: Option[OffsetDateTime]): Task[SynapseWatermark] = for
    _  <- ZIO.when(startTime.isEmpty)(ZIO.fail(new IllegalArgumentException("Backfill start date is not set")))
    wm <- dataProvider.getWatermark(startTime.get)
  yield wm

  /** Most recent version of the dataset at a time when a backfill was initiated.
    */
  override def getSnapshotVersion: Task[SynapseWatermark] = dataProvider.getCurrentVersion(SynapseWatermark.epoch)
