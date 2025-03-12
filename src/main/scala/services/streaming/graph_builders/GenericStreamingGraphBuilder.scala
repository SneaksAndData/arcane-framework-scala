package com.sneaksanddata.arcane.framework
package services.streaming.graph_builders

import models.{DataRow, given_MetadataEnrichedRowStreamElement_DataRow}
import services.app.base.StreamLifetimeService
import services.streaming.base.{HookManager, MetadataEnrichedRowStreamElement, StreamDataProvider, StreamingGraphBuilder}
import services.streaming.processors.GenericGroupingTransformer
import services.streaming.processors.batch_processors.{BackfillMergeBatchProcessor, DisposeBatchProcessor, MergeBatchProcessor}
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}

import zio.stream.ZStream
import zio.{Tag, ZIO, ZLayer}

/**
 * Provides the complete data stream for the streaming process including all the stages and services
 * except the sink and lifetime service.
 */
class GenericStreamingGraphBuilder(streamDataProvider: StreamDataProvider,
                                   fieldFilteringProcessor: FieldFilteringTransformer,
                                   groupTransformer: GenericGroupingTransformer,
                                   stagingProcessor: StagingProcessor,
                                   mergeProcessor: MergeBatchProcessor,
                                   disposeBatchProcessor: DisposeBatchProcessor,
                                   hookManager: HookManager)
  extends StreamingGraphBuilder:

  /**
   * @inheritdoc
   */
  override type ProcessedBatch = DisposeBatchProcessor#BatchType

  /**
   * @inheritdoc
   */
  override def produce: ZStream[Any, Throwable, ProcessedBatch] =
    streamDataProvider.stream
      .via(fieldFilteringProcessor.process)
      .via(groupTransformer.process)
      .via(stagingProcessor.process(hookManager.onStagingTablesComplete, hookManager.onBatchStaged))
      .via(mergeProcessor.process)
      .via(disposeBatchProcessor.process)

object GenericStreamingGraphBuilder:

  /**
   * The environment required for the GenericBackfillGraphBuilder.
   */
  type Environment = StreamDataProvider
    & GenericGroupingTransformer
    & FieldFilteringTransformer
    & StagingProcessor
    & BackfillMergeBatchProcessor
    & DisposeBatchProcessor
    & StreamLifetimeService
    & HookManager


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
  def apply(streamDataProvider: StreamDataProvider,
            fieldFilteringProcessor: FieldFilteringTransformer,
            groupTransformer: GenericGroupingTransformer,
            stagingProcessor: StagingProcessor,
            mergeProcessor: BackfillMergeBatchProcessor,
            disposeBatchProcessor: DisposeBatchProcessor,
            hookManager: HookManager): GenericBackfillGraphBuilder =
    new GenericBackfillGraphBuilder(streamDataProvider,
      fieldFilteringProcessor,
      groupTransformer,
      stagingProcessor,
      mergeProcessor,
      disposeBatchProcessor,
      hookManager)

  /**
   * The ZLayer for the GenericBackfillGraphBuilder.
   */
  val layer: ZLayer[Environment, Nothing, StreamingGraphBuilder] =
    ZLayer {
      for
        streamDataProvider <- ZIO.service[StreamDataProvider]
        fieldFilteringProcessor <- ZIO.service[FieldFilteringTransformer]
        groupTransformer <- ZIO.service[GenericGroupingTransformer]
        stagingProcessor <- ZIO.service[StagingProcessor]
        mergeProcessor <- ZIO.service[BackfillMergeBatchProcessor]
        disposeBatchProcessor <- ZIO.service[DisposeBatchProcessor]
        hookManager <- ZIO.service[HookManager]
      yield GenericBackfillGraphBuilder(streamDataProvider,
        fieldFilteringProcessor,
        groupTransformer,
        stagingProcessor,
        mergeProcessor,
        disposeBatchProcessor,
        hookManager)
    }
