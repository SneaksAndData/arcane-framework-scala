package com.sneaksanddata.arcane.framework
package services.blobsource.providers

import models.settings.{BackfillSettings, VersionedDataGraphBuilderSettings}
import services.blobsource.readers.BlobSourceReader
import services.blobsource.{BlobSourceBatch, BlobSourceVersion}
import services.streaming.base.{BackfillDataProvider, VersionedDataProvider}

import zio.{Task, ZIO, ZLayer}
import zio.stream.ZStream

import java.time.{Instant, OffsetDateTime, ZoneId, ZoneOffset}

class BlobSourceDataProvider(
    sourceReader: BlobSourceReader,
    settings: VersionedDataGraphBuilderSettings,
    backfillSettings: BackfillSettings
) extends VersionedDataProvider[BlobSourceVersion, BlobSourceBatch]
    with BackfillDataProvider[BlobSourceBatch]:

  override def requestBackfill: ZStream[Any, Throwable, BlobSourceBatch] = {
    val backFillStart =
      backfillSettings.backfillStartDate.getOrElse(OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC))
    sourceReader
      .getChanges(
        BlobSourceVersion(
          versionNumber = (backFillStart.toInstant.toEpochMilli / 1000).toString,
          waterMarkTime = backFillStart
        )
      )
  }

  override def requestChanges(previousVersion: BlobSourceVersion): ZStream[Any, Throwable, BlobSourceBatch] =
    sourceReader.getChanges(previousVersion)

  override def firstVersion: Task[BlobSourceVersion] = sourceReader.getStartFrom(settings.lookBackInterval)

  override def hasChanges(previousVersion: BlobSourceVersion): Task[Boolean] = sourceReader.hasChanges(previousVersion)

  override def getCurrentVersion(previousVersion: BlobSourceVersion): Task[BlobSourceVersion] =
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
