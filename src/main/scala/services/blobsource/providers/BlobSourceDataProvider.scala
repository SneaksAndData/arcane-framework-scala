package com.sneaksanddata.arcane.framework
package services.blobsource.providers

import models.settings.{BackfillSettings, VersionedDataGraphBuilderSettings}
import services.blobsource.readers.BlobSourceReader
import services.blobsource.{BlobSourceBatch, BlobSourceVersion, BlobSourceVersionedBatch}
import services.streaming.base.{BackfillDataProvider, VersionedDataProvider}

import zio.{Task, ZIO, ZLayer}
import zio.stream.ZStream

class BlobSourceDataProvider(
    sourceReader: BlobSourceReader,
    settings: VersionedDataGraphBuilderSettings,
    backfillSettings: BackfillSettings
) extends VersionedDataProvider[BlobSourceVersion, BlobSourceBatch]
    with BackfillDataProvider[BlobSourceBatch]:

  override def requestBackfill: ZStream[Any, Throwable, BlobSourceBatch] = {
    val backFillStart = backfillSettings.backfillStartDate.getOrElse( )
    sourceReader
      .getChanges(BlobSourceVersion(versionNumber = backfillSettings.backfillStartDate.map(_.toInstant.toEpochMilli / 1000).getOrElse(0L).toString, waterMarkTime = backfillSettings.backfillStartDate.getOrElse()))
      .map(_._1)
  }

  override def requestChanges(previousVersion: BlobSourceVersion): ZStream[Any, Throwable, BlobSourceBatch] =
    sourceReader.getChanges(previousVersion)

  override def firstVersion: Task[BlobSourceVersion] = sourceReader.getStartFrom(settings.lookBackInterval)

  override def hasChanges(previousVersion: BlobSourceVersion): Task[Boolean] = ???

  override def getCurrentVersion(previousVersion: BlobSourceVersion): Task[BlobSourceVersion] = sourceReader.getLatestVersion

object BlobSourceDataProvider:
  private type Environment = VersionedDataGraphBuilderSettings & BackfillSettings & BlobSourceReader

  val layer: ZLayer[Environment, Throwable, BlobSourceDataProvider] = ZLayer {
    for
      versionedSettings <- ZIO.service[VersionedDataGraphBuilderSettings]
      backfillSettings  <- ZIO.service[BackfillSettings]
      blobSource        <- ZIO.service[BlobSourceReader]
    yield BlobSourceDataProvider(blobSource, versionedSettings, backfillSettings)
  }
