package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors

import logging.ZIOLogAnnotations.*
import services.base.DisposeServiceClient
import services.streaming.base.StagedBatchProcessor

import com.sneaksanddata.arcane.framework.models.settings.StagingDataSettings
import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/**
 * Processor that merges data into a target table.
 */
class DisposeBatchProcessor(disposeServiceClient: DisposeServiceClient,
                            stagingDataSettings: StagingDataSettings)
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
   * @param disposeServiceClient The JDBC consumer.
   * @return The initialized MergeProcessor instance
   */
  def apply(disposeServiceClient: DisposeServiceClient, stagingDataSettings: StagingDataSettings): DisposeBatchProcessor =
    new DisposeBatchProcessor(disposeServiceClient, stagingDataSettings)

  /**
   * The required environment for the MergeBatchProcessor.
   */
  type Environment = DisposeServiceClient
  & StagingDataSettings

  /**
   * The ZLayer that creates the MergeProcessor.
   */
  val layer: ZLayer[Environment, Nothing, DisposeBatchProcessor] =
    ZLayer {
      for
        disposeServiceClient <- ZIO.service[DisposeServiceClient]
        stagingDataSettings <- ZIO.service[StagingDataSettings]
      yield DisposeBatchProcessor(disposeServiceClient, stagingDataSettings)
    }
