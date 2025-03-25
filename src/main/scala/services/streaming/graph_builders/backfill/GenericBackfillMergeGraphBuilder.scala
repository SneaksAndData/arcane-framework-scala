package com.sneaksanddata.arcane.framework
package services.streaming.graph_builders.backfill

import services.streaming.base.{BackfillStreamingGraphBuilder, BackfillStreamingMergeDataProvider, StreamDataProvider}
import services.streaming.processors.batch_processors.backfill.BackfillApplyBatchProcessor

import zio.stream.ZStream
import zio.{ZIO, ZLayer}

/**
 * Provides the complete data stream for the streaming process including all the stages and services
 * except the sink and lifetime service.
 *
 * This graph builder is used for running a backfill process when backfill behavior is set to merge.
 * The graph builder works in the following way:
  * 1. Runs the regular backfilling process targeting the target table.
 *  2. Awaits the backfilling process to complete.
 *  
 *  This graph builder does not produce any value.
 */
class GenericBackfillMergeGraphBuilder(streamDataProvider: BackfillStreamingMergeDataProvider) extends BackfillStreamingGraphBuilder:

  /**
   * @inheritdoc
   */
  override type ProcessedBatch = Unit

  /**
   * @inheritdoc
   */
  override def produce: ZStream[Any, Throwable, ProcessedBatch] = ZStream.fromZIO(streamDataProvider.requestBackfill)

object GenericBackfillMergeGraphBuilder:

  /**
   * The environment required for the GenericBackfillGraphBuilder.
   */
  type Environment = StreamDataProvider
    & BackfillStreamingMergeDataProvider
    & BackfillApplyBatchProcessor


  /**
   * Creates a new GenericBackfillGraphBuilder.
   * @param streamDataProvider The stream data provider.
   * @return The GenericBackfillGraphBuilder instance.
   */
  def apply(streamDataProvider: BackfillStreamingMergeDataProvider): GenericBackfillMergeGraphBuilder =
    new GenericBackfillMergeGraphBuilder(streamDataProvider)

  /**
   * The ZLayer for the GenericBackfillGraphBuilder.
   */
  val layer: ZLayer[Environment, Nothing, GenericBackfillMergeGraphBuilder] =
    ZLayer {
      for streamDataProvider <- ZIO.service[BackfillStreamingMergeDataProvider]
      yield GenericBackfillMergeGraphBuilder(streamDataProvider)
    }
