package com.sneaksanddata.arcane.framework
package services.blobsource.providers

import models.schemas.DataRow
import models.settings.backfill.BackfillSettings
import models.settings.sink.SinkSettings
import services.blobsource.readers.BlobSourceReader
import services.blobsource.versioning.BlobSourceWatermark
import services.blobsource.versioning.BlobSourceWatermark.*
import services.iceberg.base.SinkPropertyManager
import services.streaming.base.DefaultSourceDataProvider
import services.streaming.throughput.base.ThroughputShaperBuilder
import com.sneaksanddata.arcane.framework.models.settings.streaming.ChangeCaptureSettings

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.{Instant, OffsetDateTime, ZoneOffset}

class BlobSourceDataProvider(
    sourceReader: BlobSourceReader,
    sinkPropertyManager: SinkPropertyManager,
    sinkSettings: SinkSettings,
    settings: ChangeCaptureSettings,
    backfillSettings: BackfillSettings,
    throughputShaperBuilder: ThroughputShaperBuilder
) extends DefaultSourceDataProvider[BlobSourceWatermark](
      sinkPropertyManager,
      sinkSettings,
      backfillSettings,
      throughputShaperBuilder
    ):

  override def hasChanges(previousVersion: BlobSourceWatermark): Task[Boolean] =
    sourceReader.hasChanges(previousVersion)

  override def getCurrentVersion(previousVersion: BlobSourceWatermark): Task[BlobSourceWatermark] =
    sourceReader.getLatestVersion

  override protected def changeStream(previousVersion: BlobSourceWatermark): ZStream[Any, Throwable, DataRow] =
    sourceReader.getChanges(previousVersion)

  override protected def getBackfillStartWatermark(startTime: Option[OffsetDateTime]): BlobSourceWatermark =
    BlobSourceWatermark.fromEpochSecond(
      startTime.getOrElse(OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)).toInstant.toEpochMilli / 1000
    )

  override protected def backfillStream(backfillStartDate: Option[OffsetDateTime]): ZStream[Any, Throwable, DataRow] =
    sourceReader.getChanges(getBackfillStartWatermark(backfillStartDate))

object BlobSourceDataProvider:
  private type Environment = ChangeCaptureSettings & BackfillSettings & BlobSourceReader & SinkPropertyManager &
    SinkSettings & ThroughputShaperBuilder

  val layer: ZLayer[Environment, Throwable, BlobSourceDataProvider] = ZLayer {
    for
      versionedSettings <- ZIO.service[ChangeCaptureSettings]
      propertyManager   <- ZIO.service[SinkPropertyManager]
      sinkSettings      <- ZIO.service[SinkSettings]
      backfillSettings  <- ZIO.service[BackfillSettings]
      blobSource        <- ZIO.service[BlobSourceReader]
      throughputBuilder <- ZIO.service[ThroughputShaperBuilder]
    yield BlobSourceDataProvider(
      blobSource,
      propertyManager,
      sinkSettings,
      versionedSettings,
      backfillSettings,
      throughputBuilder
    )
  }
