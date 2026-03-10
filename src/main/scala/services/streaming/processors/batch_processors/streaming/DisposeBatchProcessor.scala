package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.streaming

import logging.ZIOLogAnnotations.*
import models.app.BaseStreamContext
import services.base.DisposeServiceClient
import services.streaming.base.StagedBatchProcessor

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/** Processor that merges data into a target table.
  */
class DisposeBatchProcessor(disposeServiceClient: DisposeServiceClient, streamContext: BaseStreamContext)
    extends StagedBatchProcessor:

  /** Processes the incoming data.
    *
    * @return
    *   ZPipeline (stream source for the stream graph).
    */
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] =
    ZPipeline.mapZIO(batchesSet =>
      if streamContext.IsBackfilling then
        for _ <- zlog(
            "Running in backfill mode. Skipping dispose of batch set with index %s",
            Seq(getAnnotation("processor", "DisposeBatchProcessor")),
            batchesSet.batchIndex.toString
          )
        yield batchesSet
      else
        for
          _ <- zlog(
            "Disposing batch set with index %s",
            Seq(getAnnotation("processor", "DisposeBatchProcessor")),
            batchesSet.batchIndex.toString
          )
          _ <- ZIO.foreach(batchesSet.groupedBySchema)(batch => disposeServiceClient.disposeBatch(batch))
        yield batchesSet
    )

object DisposeBatchProcessor:

  /** Factory method to create MergeProcessor
    *
    * @param DisposeServiceClient
    *   The JDBC consumer.
    * @return
    *   The initialized MergeProcessor instance
    */
  def apply(DisposeServiceClient: DisposeServiceClient, streamContext: BaseStreamContext): DisposeBatchProcessor =
    new DisposeBatchProcessor(DisposeServiceClient, streamContext)

  /** The required environment for the MergeBatchProcessor.
    */
  type Environment = DisposeServiceClient & BaseStreamContext

  /** The ZLayer that creates the MergeProcessor.
    */
  val layer: ZLayer[Environment, Nothing, DisposeBatchProcessor] =
    ZLayer {
      for
        disposeServiceClient <- ZIO.service[DisposeServiceClient]
        streamContext        <- ZIO.service[BaseStreamContext]
      yield DisposeBatchProcessor(disposeServiceClient, streamContext)
    }
