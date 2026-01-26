package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors

import logging.ZIOLogAnnotations.zlog
import models.batches.StagedBatch
import services.iceberg.IcebergS3CatalogWriter
import services.metrics.DeclaredMetrics
import services.streaming.base.OffsetDateTimeRW.rw
import services.streaming.base.{JsonWatermark, SourceWatermark}

import upickle.default.*
import zio.ZIO

import java.time.{Duration, OffsetDateTime}

case class TimestampOnlyWatermark(timestamp: OffsetDateTime) extends JsonWatermark:
  override def toJson: String = upickle.write(this)

  /**
   * Age of this watermark at the current moment of time, similar to SourceWatermark
   *
   * @return
   */
  def age: Long = Duration.between(timestamp, OffsetDateTime.now()).toSeconds
  
object  TimestampOnlyWatermark:
  implicit val rw: ReadWriter[TimestampOnlyWatermark] = macroRW

  def fromJson(value: String): TimestampOnlyWatermark = upickle.read(value)

object WatermarkProcessingExtensions:
  extension (batch: StagedBatch)
    def applyWatermark(writer: IcebergS3CatalogWriter, targetName: String, declaredMetrics: DeclaredMetrics): ZIO[Any, Throwable, Unit] =
      for _ <- ZIO.when(batch.completedWatermarkValue.isDefined) {
          for
            watermark <- ZIO.attempt(batch.completedWatermarkValue.get)
            _ <- zlog(
              "Batch %s completed stream from watermark %s, will update target watermark",
              batch.name,
              watermark
            )
            previousWatermark <- writer.getProperty(targetName, "comment")
            _                 <- writer.comment(targetName, watermark)
            _                 <- zlog(s"Updated watermark from $previousWatermark to $watermark")
            _ <- ZIO.attempt(TimestampOnlyWatermark.fromJson(watermark).age.toDouble) @@ declaredMetrics.appliedWatermarkAge
          yield ()
        }
      yield ()
