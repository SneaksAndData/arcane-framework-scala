package com.sneaksanddata.arcane.framework
package services.blobsource.providers

import models.settings.{BackfillSettings, VersionedDataGraphBuilderSettings}
import services.blobsource.BlobSourceBatch
import services.blobsource.readers.BlobSourceReader
import services.blobsource.versioning.BlobSourceWatermark
import services.streaming.base.{BackfillDataProvider, VersionedDataProvider}

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.{Instant, OffsetDateTime, ZoneId, ZoneOffset}

class BlobSourceDataProvider(
    sourceReader: BlobSourceReader,
    settings: VersionedDataGraphBuilderSettings,
    backfillSettings: BackfillSettings
) extends VersionedDataProvider[BlobSourceWatermark, BlobSourceBatch]
    with BackfillDataProvider[BlobSourceBatch]:

  override def requestBackfill: ZStream[Any, Throwable, BlobSourceBatch] = {
    val backFillStart =
      backfillSettings.backfillStartDate.getOrElse(OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC))
    sourceReader
      .getChanges(
        BlobSourceWatermark.fromEpochSecond(backFillStart.toInstant.toEpochMilli / 1000)
      )
  }

  override def requestChanges(previousVersion: BlobSourceWatermark): ZStream[Any, Throwable, BlobSourceBatch] =
    sourceReader.getChanges(previousVersion)

  override def firstVersion: Task[BlobSourceWatermark] = sourceReader.getStartFrom(settings.lookBackInterval)

  override def hasChanges(previousVersion: BlobSourceWatermark): Task[Boolean] =
    sourceReader.hasChanges(previousVersion)

  override def getCurrentVersion(previousVersion: BlobSourceWatermark): Task[BlobSourceWatermark] =
    sourceReader.getLatestVersion

object BlobSourceDataProvider:
  private type Environment = VersionedDataGraphBuilderSettings & BackfillSettings & BlobSourceReader

  val layer: ZLayer[Environment, Throwable, BlobSourceDataProvider] = ZLayer {
    for
      versionedSettings <- ZIO.service[VersionedDataGraphBuilderSettings]
      backfillSettings  <- ZIO.service[BackfillSettings]
      blobSource        <- ZIO.service[BlobSourceReader]
    yield BlobSourceDataProvider(blobSource, versionedSettings, backfillSettings)
  }
