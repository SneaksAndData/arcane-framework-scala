package com.sneaksanddata.arcane.framework
package services.streaming.base

import logging.ZIOLogAnnotations.{getAnnotation, zlog, zlogStream}
import models.schemas.{ArcaneSchema, DataRow, JsonWatermarkRow}
import models.settings.streaming.ChangeCaptureSettings
import services.metrics.DeclaredMetrics

import zio.stream.ZStream
import zio.{Task, ZIO}

import java.time.Duration
import scala.util.Random

class DefaultStreamDataProvider[WatermarkType <: SourceWatermark[String] & JsonWatermark](
    dataProvider: ChangeCaptureDataProvider[WatermarkType],
    settings: ChangeCaptureSettings,
    declaredMetrics: DeclaredMetrics
) extends StreamDataProvider:

  private val rng = Random(settings.changeCaptureJitterSeed)

  private def getNextSleepDuration: Duration =
    // Calculate range: [base - base * variation, base + base * variation]
    val baseDuration = settings.changeCaptureInterval.toMillis
    val offset       = (baseDuration * settings.changeCaptureJitterVariance * (2 * rng.nextDouble() - 1)).toLong

    Duration.ofMillis(baseDuration + offset)

  private def nextVersion(version: Task[(WatermarkType, Boolean)]) =
    for
      (previousVersion, isSeed) <- version
      // seedFlag allows to mark initial version and use it for watermark advance for rarely updated sources, on restart
      seedFlag       <- ZIO.succeed(if isSeed then false else isSeed)
      currentVersion <- dataProvider.getCurrentVersion(previousVersion)
      // each version loop we read watermark from target to report its age if source has changes
      targetWatermark   <- dataProvider.currentWatermark
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
    yield Some((currentVersion, previousVersion, targetWatermark, seedFlag) -> ZIO.succeed((currentVersion, seedFlag)))

  private def hasChanges(previousVersion: WatermarkType, currentTargetVersion: WatermarkType): Task[Boolean] =
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
            previousVersion.version,
            nextSleepDuration.toSeconds.toString
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
      _ <- ZIO.succeed(currentTargetVersion.age.toDouble) @@ declaredMetrics.watermarkAge
    yield isChanged

  override def stream: ZStream[Any, Throwable, StructuredZStream] = ZStream
    .unfoldZIO(dataProvider.currentWatermark.map(v => (v, true)))(nextVersion)
    .flatMap {
      case (current, previous, currentTarget, seedFlag) if current > previous =>
        ZStream
          .fromZIO(hasChanges(previous, currentTarget))
          .flatMap { sourceUpdated =>
            (sourceUpdated, seedFlag) match
              // if source entity has been updated, stream changes. Data provider attaches watermark to the end of the changeset
              case (true, _) => dataProvider.requestChanges(previous, current)

              // if source entity hasn't been updated despite global version change, we should emit an empty stream
              // however, this will lead to target watermark `age` growing infinitely until it eventually receives an update
              // to prevent this, we advance watermark on stream start (when target version matches version used to seed the loop)
              case (false, true) =>
                zlogStream(
                  "No updates detected on startup - will advance target watermark to %s to avoid excessive age increase",
                  current.version
                ) *> ZStream.succeed((ZStream.succeed(JsonWatermarkRow(current)), ArcaneSchema.empty()))
              case _ => ZStream.empty
          }
      case _ => ZStream.empty
    }
