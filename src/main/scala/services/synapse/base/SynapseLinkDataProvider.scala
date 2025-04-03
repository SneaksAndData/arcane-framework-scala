package com.sneaksanddata.arcane.framework
package services.synapse.base

import services.streaming.base.{BackfillDataProvider, VersionedDataProvider}
import services.synapse.{SynapseLinkBatch, SynapseLinkVersionedBatch}
import models.DataRow
import models.settings.{BackfillSettings, VersionedDataGraphBuilderSettings}

import zio.stream.ZStream
import zio.{Task, ZIO}

import java.time.format.DateTimeFormatter
import java.time.{Duration, OffsetDateTime, ZoneOffset}


class SynapseLinkDataProvider(synapseReader: SynapseLinkReader, settings: VersionedDataGraphBuilderSettings, backfillSettings: BackfillSettings) extends VersionedDataProvider[String, SynapseLinkVersionedBatch] with BackfillDataProvider[SynapseLinkBatch]:
  
  private val dateBlobPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ssX")

  override def requestChanges(previousVersion: Option[String]): ZStream[Any, Throwable, SynapseLinkVersionedBatch] = ZStream.fromZIO(firstVersion).flatMap(fv => synapseReader.getChanges(OffsetDateTime.parse(previousVersion.getOrElse(fv))))

  override def requestBackfill: ZStream[Any, Throwable, SynapseLinkBatch] = backfillSettings.backfillStartDate match
    case Some(backfillStartDate) => synapseReader.getChanges(backfillStartDate).map(_._1)
    case None => ZStream.fail(new IllegalArgumentException("Backfill start date is not set"))

  override def firstVersion: Task[String] = ZIO.succeed(
    OffsetDateTime.now()
      .minus(settings.lookBackInterval)
      .format(
        dateBlobPattern.withZone(ZoneOffset.UTC)
      )
  )
