package com.sneaksanddata.arcane.framework
package services.streaming.base

import logging.ZIOLogAnnotations.zlog
import models.app.StreamContext
import models.schemas.DataRow
import models.settings.VersionedDataGraphBuilderSettings
import services.metrics.DeclaredMetrics
import com.sneaksanddata.arcane.framework.models.settings.backfill.BackfillSettings

import zio.stream.ZStream
import zio.{Task, ZIO}

class DefaultStreamDataProvider[WatermarkType <: SourceWatermark[String], RowType <: DataRow](
    dataProvider: VersionedDataProvider[WatermarkType, RowType] & BackfillDataProvider[RowType],
    settings: VersionedDataGraphBuilderSettings,
    backfillSettings: BackfillSettings,
    streamContext: StreamContext,
    declaredMetrics: DeclaredMetrics
) extends StreamDataProvider:

  type StreamElementType = DataRow

  private def nextVersion(version: Task[WatermarkType]) =
    for
      previousVersion   <- version
      currentVersion    <- dataProvider.getCurrentVersion(previousVersion)
      hasVersionUpdated <- ZIO.succeed(currentVersion > previousVersion)
      _ <- ZIO.when(hasVersionUpdated)(
        zlog(
          "Watermark version changed from %s to %s",
          previousVersion.version,
          currentVersion.version
        )
      )
      _ <- ZIO.unless(hasVersionUpdated) {
        for _ <- zlog(
            "No changes in watermark version, next check in %s seconds, staying at %s version",
            settings.changeCaptureInterval.toSeconds.toString,
            previousVersion.version
          ) *> ZIO.sleep(zio.Duration.fromJava(settings.changeCaptureInterval))
        yield ()
      }
    yield Some((currentVersion, previousVersion) -> ZIO.succeed(currentVersion))

  private def hasChanges(previousVersion: WatermarkType): Task[Boolean] =
    for
      _         <- zlog("Checking watermark value %s for data changes", previousVersion.version)
      isChanged <- dataProvider.hasChanges(previousVersion)
      _ <- ZIO.unless(isChanged) {
        zlog(
          s"No changes in source data found between watermark value %s and current moment, next check in ${settings.changeCaptureInterval.toSeconds} seconds",
          previousVersion.version
        ) *> ZIO.sleep(
          settings.changeCaptureInterval
        )
      }
      _ <- ZIO.when(isChanged) {
        zlog(
          s"Source data has changed between watermark value %s and current moment, streaming",
          previousVersion.version
        ) *> ZIO.unit
      }
      _ <- ZIO.succeed(previousVersion.age.toDouble) @@ declaredMetrics.streamingWatermarkAge
    yield isChanged

  override def stream: ZStream[Any, Throwable, RowType] = if streamContext.IsBackfilling then
    dataProvider.requestBackfill
  else
    ZStream
      .unfoldZIO(dataProvider.firstVersion)(nextVersion)
      .flatMap {
        case (current, previous) if current > previous =>
          ZStream.whenZIO(hasChanges(previous))(dataProvider.requestChanges(previous, current))
        case _ => ZStream.empty
      }
