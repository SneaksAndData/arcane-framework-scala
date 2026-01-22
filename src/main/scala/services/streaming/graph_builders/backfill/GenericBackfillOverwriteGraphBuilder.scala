package com.sneaksanddata.arcane.framework
package services.streaming.graph_builders.backfill

import services.streaming.base.{
  BackfillStreamingGraphBuilder,
  BackfillStreamingOverwriteDataProvider,
  HookManager,
  StreamDataProvider
}
import services.streaming.processors.batch_processors.backfill.{
  BackfillApplyBatchProcessor,
  BackfillOverwriteWatermarkProcessor
}

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
    streamDataProvider: BackfillStreamingOverwriteDataProvider,
    applyBatchProcessor: BackfillApplyBatchProcessor,
    watermarkProcessor: BackfillOverwriteWatermarkProcessor
) extends BackfillStreamingGraphBuilder:

  /** @inheritdoc
    */
  override type ProcessedBatch = BackfillApplyBatchProcessor#BatchType

  /** @inheritdoc
    */
  override def produce(hookManager: HookManager): ZStream[Any, Throwable, ProcessedBatch] =
    ZStream
      .fromZIO(streamDataProvider.requestBackfill)
      .collectWhile({ case b: BackfillApplyBatchProcessor#BatchType =>
        b
      })
      .via(applyBatchProcessor.process)
      .via(watermarkProcessor.process)

object GenericBackfillOverwriteGraphBuilder:

  /** The environment required for the GenericBackfillGraphBuilder.
    */
  type Environment = StreamDataProvider & BackfillStreamingOverwriteDataProvider & BackfillApplyBatchProcessor &
    BackfillOverwriteWatermarkProcessor

  /** Creates a new GenericBackfillGraphBuilder.
    * @param streamDataProvider
    *   The stream data provider.
    * @param fieldFilteringProcessor
    *   The field filtering processor.
    * @param groupTransformer
    *   The group transformer.
    * @param stagingProcessor
    *   The staging processor.
    * @param mergeProcessor
    *   The merge processor.
    * @param disposeBatchProcessor
    *   The dispose batch processor.
    * @param hookManager
    *   The hook manager.
    * @return
    *   The GenericBackfillGraphBuilder instance.
    */
  def apply(
      streamDataProvider: BackfillStreamingOverwriteDataProvider,
      mergeBatchProcessor: BackfillApplyBatchProcessor,
      watermarkProcessor: BackfillOverwriteWatermarkProcessor
  ): GenericBackfillOverwriteGraphBuilder =
    new GenericBackfillOverwriteGraphBuilder(streamDataProvider, mergeBatchProcessor, watermarkProcessor)

  /** The ZLayer for the GenericBackfillGraphBuilder.
    */
  val layer: ZLayer[Environment, Nothing, GenericBackfillOverwriteGraphBuilder] =
    ZLayer {
      for
        streamDataProvider <- ZIO.service[BackfillStreamingOverwriteDataProvider]
        mergeProcessor     <- ZIO.service[BackfillApplyBatchProcessor]
        watermarkProcessor <- ZIO.service[BackfillOverwriteWatermarkProcessor]
      yield GenericBackfillOverwriteGraphBuilder(streamDataProvider, mergeProcessor, watermarkProcessor)
    }
