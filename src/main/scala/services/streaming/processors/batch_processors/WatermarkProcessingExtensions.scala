package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors

import logging.ZIOLogAnnotations.zlog
import models.batches.StagedBatch
import services.iceberg.IcebergS3CatalogWriter
import services.metrics.DeclaredMetrics
import services.streaming.base.OffsetDateTimeRW.rw
import services.streaming.base.{JsonWatermark, SourceWatermark, TimestampOnlyWatermark}

import com.sneaksanddata.arcane.framework.services.iceberg.base.TablePropertyManager
import upickle.default.*
import zio.ZIO

import java.time.{Duration, OffsetDateTime}

object WatermarkProcessingExtensions:
  extension (batch: StagedBatch)
    def applyWatermark(
                        propertyManager: TablePropertyManager,
                        targetName: String,
                        declaredMetrics: DeclaredMetrics
    ): ZIO[Any, Throwable, Unit] =
      for _ <- ZIO.when(batch.completedWatermarkValue.isDefined) {
          for
            watermark <- ZIO.attempt(batch.completedWatermarkValue.get)
            _ <- zlog(
              "Batch %s completed stream from watermark %s, will update target watermark",
              batch.name,
              watermark
            )
            previousWatermark <- propertyManager.getProperty(targetName, "comment")
            _                 <- propertyManager.comment(targetName, watermark)
            _                 <- zlog(s"Updated watermark from $previousWatermark to $watermark")
            _ <- ZIO.attempt(
              TimestampOnlyWatermark.fromJson(watermark).age.toDouble
            ) @@ declaredMetrics.appliedWatermarkAge
          yield ()
        }
      yield ()
