package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors

import logging.ZIOLogAnnotations.zlog
import models.batches.StagedBatch
import services.iceberg.IcebergS3CatalogWriter

import zio.ZIO

object WatermarkProcessingExtensions:
  extension (batch: StagedBatch)
    def applyWatermark(writer: IcebergS3CatalogWriter, targetName: String): ZIO[Any, Throwable, Unit] =
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
          yield ()
        }
      yield ()
