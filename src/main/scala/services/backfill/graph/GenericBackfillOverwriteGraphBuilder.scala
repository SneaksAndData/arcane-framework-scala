package com.sneaksanddata.arcane.framework
package services.backfill.graph

import services.backfill.BackfillStreamDataProvider
import services.streaming.base.StreamDataProvider
import services.streaming.processors.batch_processors.backfill.{BackfillApplyBatchProcessor, BackfillOverwriteWatermarkProcessor}

import zio.stream.ZStream
import zio.{ZIO, ZLayer}

/** Provides the complete data stream for the streaming process including all the stages and services except the sink
  * and lifetime service.
  *
  * This graph builder is used for running a backfill process when backfill behavior is set to overwrite. The graph
  * builder works in the following way:
  *   1. Creates the backfilling stream data provider that manages the backfilling process and produces the mergeable
  *      batch targeting the intermediate table used as target for backfill stream.
  *   2. Applying the resulting batch to the target table using the SQL `CREATE OR REPLACE TABLE` statement.
  *   3. Since the table is replaced, the dispose batch processor is not needed and the graph builder.
  */
class GenericBackfillOverwriteGraphBuilder(
    streamDataProvider: BackfillStreamDataProvider,
    applyBatchProcessor: BackfillApplyBatchProcessor,
    watermarkProcessor: BackfillOverwriteWatermarkProcessor
) extends BackfillStreamingGraphBuilder:

  /** @inheritdoc
    */
  override type ProcessedBatch = BackfillApplyBatchProcessor#BatchType

  /** @inheritdoc
    */
  override def produce(): ZStream[Any, Throwable, ProcessedBatch] =
    ZStream
      .fromZIO(streamDataProvider.backfill)
      .collectWhile({ case b: BackfillApplyBatchProcessor#BatchType =>
        b
      })
      .via(applyBatchProcessor.process)
      .via(watermarkProcessor.process)

object GenericBackfillOverwriteGraphBuilder:

  /** The environment required for the GenericBackfillGraphBuilder.
    */
  type Environment = BackfillStreamDataProvider & BackfillApplyBatchProcessor &
    BackfillOverwriteWatermarkProcessor

  /** Creates a new GenericBackfillGraphBuilder.
    */
  def apply(
      streamDataProvider: BackfillStreamDataProvider,
      mergeBatchProcessor: BackfillApplyBatchProcessor,
      watermarkProcessor: BackfillOverwriteWatermarkProcessor
  ): GenericBackfillOverwriteGraphBuilder =
    new GenericBackfillOverwriteGraphBuilder(streamDataProvider, mergeBatchProcessor, watermarkProcessor)

  /** The ZLayer for the GenericBackfillGraphBuilder.
    */
  val layer: ZLayer[Environment, Nothing, GenericBackfillOverwriteGraphBuilder] =
    ZLayer {
      for
        streamDataProvider <- ZIO.service[BackfillStreamDataProvider]
        mergeProcessor     <- ZIO.service[BackfillApplyBatchProcessor]
        watermarkProcessor <- ZIO.service[BackfillOverwriteWatermarkProcessor]
      yield GenericBackfillOverwriteGraphBuilder(streamDataProvider, mergeProcessor, watermarkProcessor)
    }
