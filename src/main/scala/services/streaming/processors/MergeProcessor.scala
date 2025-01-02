package com.sneaksanddata.arcane.framework
package services.streaming.processors

import models.querygen.MergeQuery
import services.consumers.{BatchApplicationResult, JdbcConsumer, StagedBatch, StagedVersionedBatch}
import services.streaming.base.BatchProcessor

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/**
 * Processor that merges data into a target table.
 *
 * @param jdbcConsumer The JDBC consumer.
 */
class MergeProcessor(jdbcConsumer: JdbcConsumer[StagedVersionedBatch])
  extends BatchProcessor[StagedBatch[MergeQuery], BatchApplicationResult]:

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  override def process: ZPipeline[Any, Throwable, StagedVersionedBatch, BatchApplicationResult] =
    ZPipeline.mapZIO(batch => ZIO.fromFuture(implicit ec => jdbcConsumer.applyBatch(batch)))

object MergeProcessor:

  /**
   * Factory method to create MergeProcessor
   * @param jdbcConsumer The JDBC consumer.
   * @return The initialized MergeProcessor instance
   */
  def apply(jdbcConsumer: JdbcConsumer[StagedVersionedBatch]): MergeProcessor =
    new MergeProcessor(jdbcConsumer)

  /**
   * The required environment for the MergeProcessor.
   */
  type Environment = JdbcConsumer[StagedVersionedBatch]

  /**
   * The ZLayer that creates the MergeProcessor.
   */
  val layer: ZLayer[Environment, Nothing, MergeProcessor] =
    ZLayer {
      for
        jdbcConsumer <- ZIO.service[JdbcConsumer[StagedVersionedBatch]]
      yield MergeProcessor(jdbcConsumer)
    }
