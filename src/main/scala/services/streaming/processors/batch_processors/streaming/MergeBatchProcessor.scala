package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.streaming

import logging.ZIOLogAnnotations.*
import models.app.PluginStreamContext
import services.base.MergeServiceClient
import services.metrics.DeclaredMetrics
import services.metrics.DeclaredMetrics.*
import services.streaming.base.StagedBatchProcessor

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/** Processor that merges data into a target table.
  */
class MergeBatchProcessor(
    mergeServiceClient: MergeServiceClient,
    declaredMetrics: DeclaredMetrics
) extends StagedBatchProcessor:

  /** Processes the incoming data.
    *
    * @return
    *   ZPipeline (stream source for the stream graph).
    */
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] =
    ZPipeline.mapZIO(batch =>
      (for
        _ <- zlog(
          "Applying batch %s",
          Seq(getAnnotation("processor", "MergeBatchProcessor")),
          batch.name
        )
        _ <- ZIO.unless(batch.isEmpty)(mergeServiceClient.applyBatch(batch))
      yield batch).gaugeDuration(declaredMetrics.batchMergeDuration)
    )

object MergeBatchProcessor:

  /** Factory method to create MergeProcessor
    */
  def apply(
      mergeServiceClient: MergeServiceClient,
      declaredMetrics: DeclaredMetrics
  ): MergeBatchProcessor =
    new MergeBatchProcessor(
      mergeServiceClient,
      declaredMetrics
    )

  /** The required environment for the MergeBatchProcessor.
    */
  type Environment = MergeServiceClient & PluginStreamContext & DeclaredMetrics

  /** The ZLayer that creates the MergeProcessor.
    */
  val layer: ZLayer[Environment, Nothing, MergeBatchProcessor] =
    ZLayer {
      for
        context         <- ZIO.service[PluginStreamContext]
        jdbcConsumer    <- ZIO.service[MergeServiceClient]
        declaredMetrics <- ZIO.service[DeclaredMetrics]
      yield MergeBatchProcessor(
        jdbcConsumer,
        declaredMetrics
      )
    }
