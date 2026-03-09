package com.sneaksanddata.arcane.framework
package services.streaming.base

import logging.ZIOLogAnnotations.{getAnnotation, zlog}
import models.app.BaseStreamContext
import models.schemas.DataRow
import models.settings.backfill.BackfillSettings
import models.settings.streaming.ChangeCaptureSettings
import services.metrics.DeclaredMetrics

import zio.stream.ZStream
import zio.{Task, ZIO}

import java.time.Duration
import scala.util.Random

class DefaultStreamDataProvider[WatermarkType <: SourceWatermark[String], RowType <: DataRow](
    dataProvider: VersionedDataProvider[WatermarkType, RowType] & BackfillDataProvider[RowType],
    settings: ChangeCaptureSettings,
    backfillSettings: BackfillSettings,
    streamContext: BaseStreamContext,
    declaredMetrics: DeclaredMetrics
) extends StreamDataProvider:

  type StreamElementType = DataRow

  private val rng = Random(settings.changeCaptureJitterSeed)

  private def getNextSleepDuration: Duration =
    // Calculate range: [base - base * variation, base + base * variation]
    val baseDuration = settings.changeCaptureInterval.toMillis
    val offset       = (baseDuration * settings.changeCaptureJitterVariance * (2 * rng.nextDouble() - 1)).toLong

    Duration.ofMillis(baseDuration + offset)

  private def nextVersion(version: Task[WatermarkType]) =
    for
      previousVersion   <- version
      currentVersion    <- dataProvider.getCurrentVersion(previousVersion)
      hasVersionUpdated <- ZIO.succeed(currentVersion > previousVersion)
      _ <- ZIO.when(hasVersionUpdated)(
        zlog(
          "Watermark version changed from %s to %s",
          Seq(getAnnotation("processor", "StreamProcessor")),
          previousVersion.version,
          currentVersion.version
        )
      )
      _ <- ZIO.unless(hasVersionUpdated) {
        for
          nextSleepDuration <- ZIO.succeed(getNextSleepDuration)
          _ <- zlog(
            "No changes in watermark version, next check in %s seconds, staying at %s version",
            Seq(getAnnotation("processor", "StreamProcessor")),
            nextSleepDuration.toSeconds.toString,
            previousVersion.version
          ) *> ZIO.sleep(nextSleepDuration)
        yield ()
      }
    yield Some((currentVersion, previousVersion) -> ZIO.succeed(currentVersion))

  private def hasChanges(previousVersion: WatermarkType): Task[Boolean] =
    for
      _ <- zlog(
        "Checking watermark value %s for data changes",
        Seq(getAnnotation("processor", "StreamProcessor")),
        previousVersion.version
      )
      isChanged <- dataProvider.hasChanges(previousVersion)
      _ <- ZIO.unless(isChanged) {
        for
          nextSleepDuration <- ZIO.succeed(getNextSleepDuration)
          _ <- zlog(
            "No changes in source data found between watermark value %s and current moment, next check in %s seconds",
            Seq(getAnnotation("processor", "StreamProcessor")),
            nextSleepDuration.toSeconds.toString,
            previousVersion.version
          ) *> ZIO.sleep(
            nextSleepDuration
          )
        yield ()
      }
      _ <- ZIO.when(isChanged) {
        zlog(
          "Source data has changed between watermark value %s and current moment, streaming",
          Seq(getAnnotation("processor", "StreamProcessor")),
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
