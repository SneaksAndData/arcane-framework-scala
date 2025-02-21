package com.sneaksanddata.arcane.framework
package services.streaming.processors

import models.querygen.MergeQuery
import services.base.{BatchApplicationResult, MergeServiceClient}
import services.consumers.StagedVersionedBatch
import services.streaming.base.BatchProcessor

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/**
 * Processor that merges data into a target table.
 *
 * @param client The JDBC consumer.
 */
class MergeProcessor(client: MergeServiceClient)
  extends BatchProcessor[StagedVersionedBatch, BatchApplicationResult]:

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  override def process: ZPipeline[Any, Throwable, StagedVersionedBatch, BatchApplicationResult] =
    ZPipeline.mapZIO(batch => client.applyBatch(batch))

object MergeProcessor:

  /**
   * Factory method to create MergeProcessor
   * @param client The JDBC consumer.
   * @return The initialized MergeProcessor instance
   */
  def apply(client: MergeServiceClient): MergeProcessor =
    new MergeProcessor(client)

  /**
   * The required environment for the MergeProcessor.
   */
  private type Environment = MergeServiceClient

  /**
   * The ZLayer that creates the MergeProcessor.
   */
  val layer: ZLayer[Environment, Nothing, MergeProcessor] =
    ZLayer {
      for
        client <- ZIO.service[MergeServiceClient]
      yield MergeProcessor(client)
    }
