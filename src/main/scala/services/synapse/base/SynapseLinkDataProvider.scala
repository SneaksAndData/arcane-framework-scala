package com.sneaksanddata.arcane.framework
package services.synapse.base

import models.settings.{BackfillSettings, VersionedDataGraphBuilderSettings}
import services.streaming.base.{BackfillDataProvider, VersionedDataProvider}
import services.synapse.{SynapseLinkBatch, SynapseLinkVersionedBatch}

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.format.DateTimeFormatter
import java.time.{OffsetDateTime, ZoneOffset}

class SynapseLinkDataProvider(
    synapseReader: SynapseLinkReader,
    settings: VersionedDataGraphBuilderSettings,
    backfillSettings: BackfillSettings
) extends VersionedDataProvider[String, SynapseLinkVersionedBatch]
    with BackfillDataProvider[SynapseLinkBatch]:

  private val dateBlobPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ssX")

  override def requestChanges(previousVersion: String): ZStream[Any, Throwable, SynapseLinkVersionedBatch] =
    synapseReader.getChanges(OffsetDateTime.parse(previousVersion, dateBlobPattern))

  override def requestBackfill: ZStream[Any, Throwable, SynapseLinkBatch] = backfillSettings.backfillStartDate match
    case Some(backfillStartDate) => synapseReader.getChanges(backfillStartDate).map(_._1)
    case None                    => ZStream.fail(new IllegalArgumentException("Backfill start date is not set"))

  override def firstVersion: Task[String] = ZIO.succeed(
    OffsetDateTime
      .now()
      .minus(settings.lookBackInterval)
      .format(
        dateBlobPattern.withZone(ZoneOffset.UTC)
      )
  )

object SynapseLinkDataProvider:
  type Environment = VersionedDataGraphBuilderSettings & BackfillSettings & SynapseLinkReader

  val layer: ZLayer[Environment, Throwable, SynapseLinkDataProvider] = ZLayer {
    for
      versionedSettings <- ZIO.service[VersionedDataGraphBuilderSettings]
      backfillSettings  <- ZIO.service[BackfillSettings]
      synapseReader     <- ZIO.service[SynapseLinkReader]
    yield SynapseLinkDataProvider(synapseReader, versionedSettings, backfillSettings)
  }
