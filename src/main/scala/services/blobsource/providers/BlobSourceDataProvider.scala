package com.sneaksanddata.arcane.framework
package services.blobsource.providers

import logging.ZIOLogAnnotations.zlog
import models.schemas.JsonWatermarkRow
import models.settings.{BackfillSettings, TargetTableSettings, VersionedDataGraphBuilderSettings}
import services.blobsource.BlobSourceBatch
import services.blobsource.readers.BlobSourceReader
import services.blobsource.versioning.BlobSourceWatermark
import services.iceberg.IcebergS3CatalogWriter
import services.streaming.base.{BackfillDataProvider, VersionedDataProvider}

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.{Instant, OffsetDateTime, ZoneOffset}
import scala.util.Try

class BlobSourceDataProvider(
    sourceReader: BlobSourceReader,
    icebergS3CatalogWriter: IcebergS3CatalogWriter,
    targetTableSettings: TargetTableSettings,
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
      watermarkString <- icebergS3CatalogWriter.getProperty(targetTableSettings.targetTableNameParts.Name, "comment")
      _ <- zlog("Current watermark value on %s is '%s'", targetTableSettings.targetTableFullName, watermarkString)
      watermark <- ZIO.attempt(Try(BlobSourceWatermark.fromJson(watermarkString)).toOption)
      fallback <- ZIO.when(watermark.isEmpty) {
        sourceReader.getStartFrom(settings.lookBackInterval)
      }
    // assume fallback is only there if we have no watermark
    yield fallback match {
      // if fallback is computed, return it
      case Some(value) => value
      // if no fallback, get value from watermark and fail if it is empty
      case None => watermark.get
    }

  override def hasChanges(previousVersion: BlobSourceWatermark): Task[Boolean] =
    sourceReader.hasChanges(previousVersion)

  override def getCurrentVersion(previousVersion: BlobSourceWatermark): Task[BlobSourceWatermark] =
    sourceReader.getLatestVersion

object BlobSourceDataProvider:
  private type Environment = VersionedDataGraphBuilderSettings & BackfillSettings & BlobSourceReader &
    IcebergS3CatalogWriter & TargetTableSettings

  val layer: ZLayer[Environment, Throwable, BlobSourceDataProvider] = ZLayer {
    for
      versionedSettings      <- ZIO.service[VersionedDataGraphBuilderSettings]
      icebergS3CatalogWriter <- ZIO.service[IcebergS3CatalogWriter]
      targetTableSettings    <- ZIO.service[TargetTableSettings]
      backfillSettings       <- ZIO.service[BackfillSettings]
      blobSource             <- ZIO.service[BlobSourceReader]
    yield BlobSourceDataProvider(
      blobSource,
      icebergS3CatalogWriter,
      targetTableSettings,
      versionedSettings,
      backfillSettings
    )
  }
