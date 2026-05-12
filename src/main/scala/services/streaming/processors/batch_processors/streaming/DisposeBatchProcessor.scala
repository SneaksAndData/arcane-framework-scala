package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.streaming

import logging.ZIOLogAnnotations.*
import models.app.{BaseStreamContext, PluginStreamContext}
import services.base.DisposeServiceClient
import services.streaming.base.StagedBatchProcessor

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/** Processor that merges data into a target table.
  */
class DisposeBatchProcessor(disposeServiceClient: DisposeServiceClient, isBackfilling: Boolean)
    extends StagedBatchProcessor:

  /** Processes the incoming data.
    *
    * @return
    *   ZPipeline (stream source for the stream graph).
    */
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] =
    ZPipeline.mapZIO { batch =>
      for
        _ <- ZIO.when(isBackfilling) {
          for _ <- zlog(
              "Running in backfill mode. Skipping dispose of a batch %s",
              Seq(getAnnotation("processor", "DisposeBatchProcessor")),
              batch.name
            )
          yield ()
        }
        _ <- ZIO.unless(isBackfilling) {
          for
            _ <- zlog(
              "Disposing a batch %s",
              Seq(getAnnotation("processor", "DisposeBatchProcessor")),
              batch.name
            )
            _ <- disposeServiceClient.disposeBatch(batch)
          yield ()
        }
      yield batch
    }

object DisposeBatchProcessor:

  /** Factory method to create MergeProcessor
    *
    * @param DisposeServiceClient
    *   The JDBC consumer.
    * @return
    *   The initialized MergeProcessor instance
    */
  def apply(DisposeServiceClient: DisposeServiceClient, isBackfilling: Boolean): DisposeBatchProcessor =
    new DisposeBatchProcessor(DisposeServiceClient, isBackfilling)

  /** The required environment for the MergeBatchProcessor.
    */
  type Environment = DisposeServiceClient & PluginStreamContext

  /** The ZLayer that creates the MergeProcessor.
    */
  val layer: ZLayer[Environment, Throwable, DisposeBatchProcessor] =
    ZLayer {
      for
        disposeServiceClient <- ZIO.service[DisposeServiceClient]
        streamContext        <- ZIO.service[PluginStreamContext]
        isBackfilling        <- streamContext.isBackfilling
      yield DisposeBatchProcessor(disposeServiceClient, isBackfilling)
    }
