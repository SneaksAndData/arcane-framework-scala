package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.streaming

import logging.ZIOLogAnnotations.zlog
import services.iceberg.IcebergS3CatalogWriter
import services.streaming.base.*
import services.streaming.processors.transformers.IndexedStagedBatches

import zio.ZIO
import zio.stream.ZPipeline

class WatermarkProcessor(icebergS3CatalogWriter: IcebergS3CatalogWriter) extends StagedBatchProcessor:
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] =     ZPipeline.mapZIO { batchesSet =>
    for
      _ <- ZIO.foreach(batchesSet.groupedBySchema) { batch =>
        ZIO.when(batch.completedWatermarkValue.isDefined) {
          for
            watermark <- ZIO.succeed(batch.completedWatermarkValue.get)
            _ <- zlog(s"Batch ${batch.name} completed stream from watermark $watermark, will updating target watermark")
          yield ()  
        }  
      }
    yield batchesSet  
  }
