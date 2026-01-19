package com.sneaksanddata.arcane.framework
package services.streaming.base

import models.app.StreamContext
import models.schemas.DataRow
import models.settings.{BackfillSettings, VersionedDataGraphBuilderSettings}
import services.blobsource.providers.BlobSourceDataProvider

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import zio.{Task, ZIO}
import zio.stream.ZStream


class DefaultStreamDataProvider[WatermarkType <: SourceWatermark[String], RowType <: DataRow](    dataProvider: VersionedDataProvider[WatermarkType, RowType] & BackfillDataProvider[RowType],
                                    settings: VersionedDataGraphBuilderSettings,
                                    backfillSettings: BackfillSettings,
                                    streamContext: StreamContext) extends StreamDataProvider:

  type StreamElementType = DataRow
  
  private def nextVersion(version: Task[WatermarkType]) =
    for
      previousVersion   <- version
      currentVersion    <- dataProvider.getCurrentVersion(previousVersion)
      hasVersionUpdated <- ZIO.succeed(currentVersion > previousVersion)
      _ <- ZIO.when(hasVersionUpdated) {
        for
          _ <- zlog(
            "Watermark version updated from %s to %s, checking for changes",
            previousVersion.version,
            currentVersion.version
          )
          _ <- checkEmpty(previousVersion)
        yield ()
      }
      _ <- ZIO.unless(hasVersionUpdated) {
        for _ <- zlog(
          "No changes in watermark version, next check in %s seconds, staying at %s version",
          settings.changeCaptureInterval.toSeconds.toString,
          previousVersion.version
        ) *> ZIO.sleep(zio.Duration.fromJava(settings.changeCaptureInterval))
        yield ()
      }
    yield Some((currentVersion, previousVersion) -> ZIO.succeed(currentVersion))

  private def checkEmpty(previousVersion: WatermarkType): Task[Unit] =
    for
      _         <- zlog(s"Received versioned batch: ${previousVersion.version}")
      isChanged <- dataProvider.hasChanges(previousVersion)
      _ <- ZIO.unless(isChanged) {
        zlog(
          s"No data in the batch, next check in ${settings.changeCaptureInterval.toSeconds} seconds"
        ) *> ZIO.sleep(
          settings.changeCaptureInterval
        )
      }
      _ <- ZIO.when(isChanged) {
        zlog(s"Data found in the batch: ${previousVersion.version}, continuing") *> ZIO.unit
      }
    yield ()
    

  override def stream: ZStream[Any, Throwable, RowType] = if streamContext.IsBackfilling then {
    // pending https://github.com/SneaksAndData/arcane-framework-scala/issues/181 to avoid asInstanceOf
    dataProvider.requestBackfill
  } else
    ZStream
      .unfoldZIO(dataProvider.firstVersion)(nextVersion)
      .flatMap {
        case (current, previous) if current > previous => dataProvider.requestChanges(previous)
        case _ => ZStream.empty
      }
