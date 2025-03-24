package com.sneaksanddata.arcane.framework
package services.streaming.graph_builders

import models.{DataRow, given_MetadataEnrichedRowStreamElement_DataRow}
import services.app.base.StreamLifetimeService
import services.streaming.base.{BackfillStreamingDataProvider, BackfillStreamingGraphBuilder, HookManager, MetadataEnrichedRowStreamElement, StreamDataProvider, StreamingGraphBuilder}
import services.streaming.processors.GenericGroupingTransformer
import services.streaming.processors.batch_processors.{BackfillDisposeBatchProcessor, BackfillApplyBatchProcessor}
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}

import zio.stream.ZStream
import zio.{Tag, ZIO, ZLayer}

/**
 * Provides the complete data stream for the streaming process including all the stages and services
 * except the sink and lifetime service.
 *
 * This graph builder is used for running a backfill process when backfill behavior is set to overwrite.
 * The graph builder works in the following way:
 * 1. Creates the backfilling stream data provider that manages the backfilling process and produces the
 *    mergeable batch targeting the intermediate table used as target for backfill stream.
 * 2. Applying the resulting batch to the target table using the SQL `CREATE OR REPLACE TABLE` statement.
 * 3. Since the table is replaced, the dispose batch processor is not needed and the graph builder.
 */
class GenericBackfillOverwriteGraphBuilder(streamDataProvider: BackfillStreamingDataProvider,
                                           applyBatchProcessor: BackfillApplyBatchProcessor)
  extends BackfillStreamingGraphBuilder:

  /**
   * @inheritdoc
   */
  override type ProcessedBatch = BackfillDisposeBatchProcessor#BatchType

  /**
   * @inheritdoc
   */
  override def produce: ZStream[Any, Throwable, ProcessedBatch] = 
    ZStream.fromZIO(streamDataProvider.requestBackfill).via(applyBatchProcessor.process)

object GenericBackfillOverwriteGraphBuilder:

  /**
   * The environment required for the GenericBackfillGraphBuilder.
   */
  type Environment = StreamDataProvider
    & BackfillStreamingDataProvider 
    & BackfillApplyBatchProcessor


  /**
   * Creates a new GenericBackfillGraphBuilder.
   * @param streamDataProvider The stream data provider.
   * @param fieldFilteringProcessor The field filtering processor.
   * @param groupTransformer The group transformer.
   * @param stagingProcessor The staging processor.
   * @param mergeProcessor The merge processor.
   * @param disposeBatchProcessor The dispose batch processor.
   * @param hookManager The hook manager.
   * @return The GenericBackfillGraphBuilder instance.
   */
  def apply(streamDataProvider: BackfillStreamingDataProvider,
            mergeBatchProcessor: BackfillApplyBatchProcessor): GenericBackfillOverwriteGraphBuilder =
    new GenericBackfillOverwriteGraphBuilder(streamDataProvider, mergeBatchProcessor)
    
  /**
   * The ZLayer for the GenericBackfillGraphBuilder.
   */
  val layer: ZLayer[Environment, Nothing, BackfillStreamingGraphBuilder] =
    ZLayer {
      for
        streamDataProvider <- ZIO.service[BackfillStreamingDataProvider]
        mergeProcessor <- ZIO.service[BackfillApplyBatchProcessor]
      yield GenericBackfillOverwriteGraphBuilder(streamDataProvider, mergeProcessor)
    }
