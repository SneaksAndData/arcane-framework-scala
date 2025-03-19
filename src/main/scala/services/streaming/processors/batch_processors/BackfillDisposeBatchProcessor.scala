package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors

import logging.ZIOLogAnnotations.*
import services.base.DisposeServiceClient
import services.streaming.base.{BatchProcessor, StagedBatchProcessor, StreamingBatchProcessor}

import com.sneaksanddata.arcane.framework.services.consumers.StagedBackfillBatch
import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/**
 * Processor that merges data into a target table.
 */
class BackfillDisposeBatchProcessor(disposeServiceClient: DisposeServiceClient)
  extends StreamingBatchProcessor:

  override type BatchType = StagedBackfillBatch
  
  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] =
    ZPipeline.mapZIO(batch =>
      for _ <- zlog(s"Disposing batch with name ${batch.name}")
          _ <- disposeServiceClient.disposeBatch(batch)
      yield batch
    )

object BackfillDisposeBatchProcessor:

  /**
   * Factory method to create MergeProcessor
   *
   * @param DisposeServiceClient The JDBC consumer.
   * @return The initialized MergeProcessor instance
   */
  def apply(DisposeServiceClient: DisposeServiceClient): BackfillDisposeBatchProcessor =
    new BackfillDisposeBatchProcessor(DisposeServiceClient)

  /**
   * The required environment for the BackfillMergeBatchProcessor.
   */
  type Environment = DisposeServiceClient

  /**
   * The ZLayer that creates the MergeProcessor.
   */
  val layer: ZLayer[Environment, Nothing, BackfillDisposeBatchProcessor] =
    ZLayer {
      for
        disposeServiceClient <- ZIO.service[DisposeServiceClient]
      yield BackfillDisposeBatchProcessor(disposeServiceClient)
    }
