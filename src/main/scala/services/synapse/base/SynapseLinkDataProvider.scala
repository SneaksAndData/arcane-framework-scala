package com.sneaksanddata.arcane.framework
package services.synapse.base

import logging.ZIOLogAnnotations.zlog
import models.schemas.JsonWatermarkRow
import models.settings.{BackfillSettings, VersionedDataGraphBuilderSettings}
import services.streaming.base.{BackfillDataProvider, VersionedDataProvider}
import services.synapse.SynapseLinkBatch
import services.synapse.versioning.SynapseWatermark

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.OffsetDateTime

class SynapseLinkDataProvider(
    synapseReader: SynapseLinkReader,
    settings: VersionedDataGraphBuilderSettings,
    backfillSettings: BackfillSettings
) extends VersionedDataProvider[SynapseWatermark, SynapseLinkBatch]
    with BackfillDataProvider[SynapseLinkBatch]:

  override def requestChanges(previousVersion: SynapseWatermark): ZStream[Any, Throwable, SynapseLinkBatch] =
    synapseReader.getChanges(previousVersion).concat(ZStream.succeed(JsonWatermarkRow(previousVersion)))

  override def requestBackfill: ZStream[Any, Throwable, SynapseLinkBatch] = backfillSettings.backfillStartDate match
    case Some(backfillStartDate) => synapseReader.getData(backfillStartDate)
    case None                    => ZStream.fail(new IllegalArgumentException("Backfill start date is not set"))

  override def firstVersion: Task[SynapseWatermark] =
    for
      startTime <- ZIO.succeed(OffsetDateTime.now())
      _ <- zlog("Fetching version for the first iteration from %s", startTime.minus(settings.lookBackInterval).toString)
      result <- synapseReader.getVersion(startTime.minus(settings.lookBackInterval))
      _      <- zlog("Retrieved version %s", result.version)
    yield result

  override def hasChanges(previousVersion: SynapseWatermark): Task[Boolean] =
    synapseReader.hasChanges(previousVersion)

  override def getCurrentVersion(previousVersion: SynapseWatermark): Task[SynapseWatermark] =
    synapseReader.getCurrentVersion(previousVersion)

object SynapseLinkDataProvider:
  type Environment = VersionedDataGraphBuilderSettings & BackfillSettings & SynapseLinkReader

  val layer: ZLayer[Environment, Throwable, SynapseLinkDataProvider] = ZLayer {
    for
      versionedSettings <- ZIO.service[VersionedDataGraphBuilderSettings]
      backfillSettings  <- ZIO.service[BackfillSettings]
      synapseReader     <- ZIO.service[SynapseLinkReader]
    yield SynapseLinkDataProvider(synapseReader, versionedSettings, backfillSettings)
  }
