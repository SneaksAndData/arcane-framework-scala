package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors

import logging.ZIOLogAnnotations.zlog
import models.batches.StagedBatch
import services.iceberg.base.TablePropertyManager
import services.metrics.DeclaredMetrics
import services.streaming.base.TimestampOnlyWatermark

import zio.ZIO

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
              "Changes associated with watermark %s has finished streaming, will update target",
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
