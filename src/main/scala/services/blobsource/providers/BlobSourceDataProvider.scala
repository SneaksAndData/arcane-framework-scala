package com.sneaksanddata.arcane.framework
package services.blobsource.providers

import models.schemas.DataRow
import models.settings.{BackfillSettings, VersionedDataGraphBuilderSettings}
import services.blobsource.readers.BlobSourceReader
import services.blobsource.{BlobSourceBatch, BlobSourceVersionedBatch}
import services.streaming.base.{BackfillDataProvider, VersionedDataProvider}

import zio.{Task, ZIO, ZLayer}
import zio.stream.ZStream

class BlobSourceDataProvider(
    sourceReader: BlobSourceReader,
    settings: VersionedDataGraphBuilderSettings,
    backfillSettings: BackfillSettings
) extends VersionedDataProvider[Long, BlobSourceVersionedBatch]
    with BackfillDataProvider[BlobSourceBatch]:

  override def requestBackfill: ZStream[Any, Throwable, BlobSourceBatch] =
    sourceReader.getChanges(backfillSettings.backfillStartDate.map(_.toInstant.toEpochMilli).getOrElse(0L)).map(_._1)

  override def requestChanges(previousVersion: Long): ZStream[Any, Throwable, BlobSourceVersionedBatch] =
    sourceReader.getChanges(previousVersion)

  override def firstVersion: Task[Long] = sourceReader.getStartFrom(settings.lookBackInterval)

  def nextVersion: Task[Long] = sourceReader.getLatestVersion

object BlobSourceDataProvider:
  private type Environment = VersionedDataGraphBuilderSettings & BackfillSettings & BlobSourceReader

  val layer: ZLayer[Environment, Throwable, BlobSourceDataProvider] = ZLayer {
    for
      versionedSettings <- ZIO.service[VersionedDataGraphBuilderSettings]
      backfillSettings  <- ZIO.service[BackfillSettings]
      blobSource        <- ZIO.service[BlobSourceReader]
    yield BlobSourceDataProvider(blobSource, versionedSettings, backfillSettings)
  }
