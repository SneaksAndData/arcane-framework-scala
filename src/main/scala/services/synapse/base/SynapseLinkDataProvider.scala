package com.sneaksanddata.arcane.framework
package services.synapse.base

import models.settings.{BackfillSettings, VersionedDataGraphBuilderSettings}
import services.streaming.base.{BackfillDataProvider, VersionedDataProvider}
import services.synapse.{SynapseBatchVersion, SynapseLinkBatch, SynapseLinkVersionedBatch}

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.services.storage.models.base.StoredBlob
import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.format.DateTimeFormatter
import java.time.{OffsetDateTime, ZoneOffset}

class SynapseLinkDataProvider(
    synapseReader: SynapseLinkReader,
    settings: VersionedDataGraphBuilderSettings,
    backfillSettings: BackfillSettings
) extends VersionedDataProvider[SynapseBatchVersion, SynapseLinkBatch]
    with BackfillDataProvider[SynapseLinkBatch]:

  private val dateBlobPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ssX")

  override def requestChanges(previousVersion: SynapseBatchVersion): ZStream[Any, Throwable, SynapseLinkBatch] =
    synapseReader.getChanges(OffsetDateTime.parse(previousVersion, dateBlobPattern))

  override def requestBackfill: ZStream[Any, Throwable, SynapseLinkBatch] = backfillSettings.backfillStartDate match
    case Some(backfillStartDate) => synapseReader.getChanges(backfillStartDate).map(_._1)
    case None                    => ZStream.fail(new IllegalArgumentException("Backfill start date is not set"))

  override def firstVersion: Task[SynapseBatchVersion] =
    for  
      startTime <- ZIO.succeed(OffsetDateTime.now())
      _ <- zlog("Fetching version for the first iteration from {startTime}", startTime.toString)
      result <- synapseReader.getCurrentVersion(SynapseBatchVersion(
    versionNumber = "", waterMarkTime = startTime.minus(settings.lookBackInterval), blob = StoredBlob.empty
  ))
      _ <- zlog("Retrieved version {firstVersion}", result.versionNumber)
    yield result
  
  override def hasChanges(previousVersion: SynapseBatchVersion): Task[Boolean] = synapseReader.hasChanges(previousVersion)
  
  override def getCurrentVersion(previousVersion: SynapseBatchVersion): Task[SynapseBatchVersion] = synapseReader.getCurrentVersion(previousVersion)

object SynapseLinkDataProvider:
  type Environment = VersionedDataGraphBuilderSettings & BackfillSettings & SynapseLinkReader

  val layer: ZLayer[Environment, Throwable, SynapseLinkDataProvider] = ZLayer {
    for
      versionedSettings <- ZIO.service[VersionedDataGraphBuilderSettings]
      backfillSettings  <- ZIO.service[BackfillSettings]
      synapseReader     <- ZIO.service[SynapseLinkReader]
    yield SynapseLinkDataProvider(synapseReader, versionedSettings, backfillSettings)
  }
