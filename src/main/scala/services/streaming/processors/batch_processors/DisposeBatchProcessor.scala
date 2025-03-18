package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors

import logging.ZIOLogAnnotations.*
import services.base.DisposeServiceClient
import services.streaming.base.StagedBatchProcessor

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/**
 * Processor that merges data into a target table.
 */
class DisposeBatchProcessor(disposeServiceClient: DisposeServiceClient)
  extends StagedBatchProcessor:

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] =
    ZPipeline.mapZIO(batchesSet =>
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
  def apply(DisposeServiceClient: DisposeServiceClient): DisposeBatchProcessor =
    new DisposeBatchProcessor(DisposeServiceClient)

  /**
   * The required environment for the MergeBatchProcessor.
   */
  type Environment = DisposeServiceClient

  /**
   * The ZLayer that creates the MergeProcessor.
   */
  val layer: ZLayer[Environment, Nothing, DisposeBatchProcessor] =
    ZLayer {
      for
        disposeServiceClient <- ZIO.service[DisposeServiceClient]
      yield DisposeBatchProcessor(disposeServiceClient)
    }
