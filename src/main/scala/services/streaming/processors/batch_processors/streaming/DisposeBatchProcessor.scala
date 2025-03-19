package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.streaming

import logging.ZIOLogAnnotations.*
import services.base.DisposeServiceClient
import services.streaming.base.StagedBatchProcessor

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/**
 * Processor that merges data into a target table.
 */
class DisposeBatchProcessor(disposeServiceClient: DisposeServiceClient, streamContext: StreamContext)
  extends StagedBatchProcessor:

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] =
    ZPipeline.mapZIO(batchesSet =>
      if streamContext.IsBackfilling then
        for _ <- zlog(s"Running in backfill mode. Skipping dispose of batch set with index ${batchesSet.batchIndex}")
        yield batchesSet
      else  
        for _ <- zlog(s"Disposing batch set with index ${batchesSet.batchIndex}")
            _ <- ZIO.foreach(batchesSet.groupedBySchema)(batch => disposeServiceClient.disposeBatch(batch))
        yield batchesSet
    )

object DisposeBatchProcessor:

  /**
   * Factory method to create MergeProcessor
   *
   * @param DisposeServiceClient The JDBC consumer.
   * @return The initialized MergeProcessor instance
   */
  def apply(DisposeServiceClient: DisposeServiceClient, streamContext: StreamContext): DisposeBatchProcessor =
    new DisposeBatchProcessor(DisposeServiceClient, streamContext)

  /**
   * The required environment for the BackfillMergeBatchProcessor.
   */
  type Environment = DisposeServiceClient
    & StreamContext

  /**
   * The ZLayer that creates the MergeProcessor.
   */
  val layer: ZLayer[Environment, Nothing, DisposeBatchProcessor] =
    ZLayer {
      for
        disposeServiceClient <- ZIO.service[DisposeServiceClient]
        streamContext <- ZIO.service[StreamContext]
      yield DisposeBatchProcessor(disposeServiceClient, streamContext)
    }
