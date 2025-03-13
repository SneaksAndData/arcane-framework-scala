package com.sneaksanddata.arcane.framework
package services.streaming.graph_builders

import models.{DataRow, given_MetadataEnrichedRowStreamElement_DataRow}
import services.app.base.StreamLifetimeService
import services.streaming.base.{BackfillStreamingDataProvider, BackfillStreamingGraphBuilder, HookManager, MetadataEnrichedRowStreamElement, StreamDataProvider, StreamingGraphBuilder}
import services.streaming.processors.GenericGroupingTransformer
import services.streaming.processors.batch_processors.{BackfillDisposeBatchProcessor, BackfillMergeBatchProcessor}
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}

import zio.stream.ZStream
import zio.{Tag, ZIO, ZLayer}

/**
 * Provides the complete data stream for the streaming process including all the stages and services
 * except the sink and lifetime service.
 */
class GenericBackfillGraphBuilder(streamDataProvider: BackfillStreamingDataProvider,
                                  mergeBatchProcessor: BackfillMergeBatchProcessor,
                                  disposeBatchProcessor: BackfillDisposeBatchProcessor)
  extends BackfillStreamingGraphBuilder:

  /**
   * @inheritdoc
   */
  override type ProcessedBatch = BackfillDisposeBatchProcessor#BatchType

  /**
   * @inheritdoc
   */
  override def produce: ZStream[Any, Throwable, ProcessedBatch] = 
    ZStream.fromZIO(streamDataProvider.requestBackfill)
      .via(mergeBatchProcessor.process)
      .via(disposeBatchProcessor.process)

object GenericBackfillGraphBuilder:

  /**
   * The environment required for the GenericBackfillGraphBuilder.
   */
  type Environment = StreamDataProvider
    & BackfillStreamingDataProvider 
    & BackfillMergeBatchProcessor
    & BackfillDisposeBatchProcessor


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
            mergeBatchProcessor: BackfillMergeBatchProcessor,
            disposeBatchProcessor: BackfillDisposeBatchProcessor): GenericBackfillGraphBuilder =
    new GenericBackfillGraphBuilder(streamDataProvider, mergeBatchProcessor, disposeBatchProcessor) 
    
  /**
   * The ZLayer for the GenericBackfillGraphBuilder.
   */
  val layer: ZLayer[Environment, Nothing, StreamingGraphBuilder] =
    ZLayer {
      for
        streamDataProvider <- ZIO.service[BackfillStreamingDataProvider]
        mergeProcessor <- ZIO.service[BackfillMergeBatchProcessor]
        disposeBatchProcessor <- ZIO.service[BackfillDisposeBatchProcessor]
      yield GenericBackfillGraphBuilder(streamDataProvider,
        mergeProcessor,
        disposeBatchProcessor)
    }
