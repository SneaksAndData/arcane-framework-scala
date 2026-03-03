package com.sneaksanddata.arcane.framework
package services.blobsource.providers

import logging.ZIOLogAnnotations.zlog
import models.schemas.JsonWatermarkRow
import models.settings.VersionedDataGraphBuilderSettings
import models.settings.backfill.BackfillSettings
import models.settings.sink.SinkSettings
import services.blobsource.BlobSourceBatch
import services.blobsource.readers.BlobSourceReader
import services.blobsource.versioning.BlobSourceWatermark
import services.iceberg.base.TablePropertyManager
import services.streaming.base.{BackfillDataProvider, VersionedDataProvider}

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.{Instant, OffsetDateTime, ZoneOffset}

class BlobSourceDataProvider(
    sourceReader: BlobSourceReader,
    propertyManager: TablePropertyManager,
    sinkSettings: SinkSettings,
    settings: VersionedDataGraphBuilderSettings,
    backfillSettings: BackfillSettings
) extends VersionedDataProvider[BlobSourceWatermark, BlobSourceBatch]
    with BackfillDataProvider[BlobSourceBatch]:

  override def requestBackfill: ZStream[Any, Throwable, BlobSourceBatch] =
    val backFillStart =
      backfillSettings.backfillStartDate.getOrElse(OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC))

    val startWatermark = BlobSourceWatermark.fromEpochSecond(backFillStart.toInstant.toEpochMilli / 1000)
    ZStream
      .fromZIO(getCurrentVersion(startWatermark))
      .flatMap(watermark =>
        sourceReader
          .getChanges(
            startWatermark
          )
          .concat(ZStream.succeed(JsonWatermarkRow(watermark)))
      )

  override def requestChanges(
      previousVersion: BlobSourceWatermark,
      nextVersion: BlobSourceWatermark
  ): ZStream[Any, Throwable, BlobSourceBatch] =
    sourceReader.getChanges(previousVersion).concat(ZStream.succeed(JsonWatermarkRow(nextVersion)))

  override def firstVersion: Task[BlobSourceWatermark] =
    for
      watermarkString <- propertyManager.getProperty(sinkSettings.targetTableNameParts.Name, "comment")
      _ <- zlog("Current watermark value on %s is '%s'", sinkSettings.targetTableFullName, watermarkString)
      watermark <- ZIO
        .attempt(BlobSourceWatermark.fromJson(watermarkString))
        .orDieWith(e =>
          new Throwable(
            s"Target contains invalid watermark: '$watermarkString'. Please run a backfill or update the watermark manually via COMMENT ON statement",
            e
          )
        )
    yield watermark

  override def hasChanges(previousVersion: BlobSourceWatermark): Task[Boolean] =
    sourceReader.hasChanges(previousVersion)

  override def getCurrentVersion(previousVersion: BlobSourceWatermark): Task[BlobSourceWatermark] =
    sourceReader.getLatestVersion

object BlobSourceDataProvider:
  private type Environment = VersionedDataGraphBuilderSettings & BackfillSettings & BlobSourceReader &
    TablePropertyManager & SinkSettings

  val layer: ZLayer[Environment, Throwable, BlobSourceDataProvider] = ZLayer {
    for
      versionedSettings <- ZIO.service[VersionedDataGraphBuilderSettings]
      propertyManager   <- ZIO.service[TablePropertyManager]
      sinkSettings      <- ZIO.service[SinkSettings]
      backfillSettings  <- ZIO.service[BackfillSettings]
      blobSource        <- ZIO.service[BlobSourceReader]
    yield BlobSourceDataProvider(
      blobSource,
      propertyManager,
      sinkSettings,
      versionedSettings,
      backfillSettings
    )
  }
