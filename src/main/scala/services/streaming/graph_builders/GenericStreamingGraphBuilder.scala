package com.sneaksanddata.arcane.framework
package services.streaming.graph_builders

import services.app.base.StreamLifetimeService
import services.streaming.base.{BackfillSubStream, HookManager, StreamDataProvider, StreamingGraphBuilder}
import services.streaming.processors.GenericGroupingTransformer
import services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor}
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
                                   disposeBatchProcessor: DisposeBatchProcessor)
  extends StreamingGraphBuilder with BackfillSubStream:

  /**
   * @inheritdoc
   */
  override type ProcessedBatch = DisposeBatchProcessor#BatchType

  /**
   * @inheritdoc
   */
  override def produce(hookManager: HookManager): ZStream[Any, Throwable, ProcessedBatch] =
    streamDataProvider.stream
      .bufferUnbounded
      .via(fieldFilteringProcessor.process)
      .via(groupTransformer.process)
      .via(stagingProcessor.process(hookManager.onStagingTablesComplete, hookManager.onBatchStaged))
      .via(mergeProcessor.process)
      .via(disposeBatchProcessor.process)

object GenericStreamingGraphBuilder:

  /**
   * The environment required for the GenericStreamingGraphBuilder.
   */
  type Environment = StreamDataProvider
    & GenericGroupingTransformer
    & FieldFilteringTransformer
    & StagingProcessor
    & MergeBatchProcessor
    & DisposeBatchProcessor
    & StreamLifetimeService


  /**
   * Creates a new GenericStreamingGraphBuilder.
   * @param streamDataProvider The stream data provider.
   * @param fieldFilteringProcessor The field filtering processor.
   * @param groupTransformer The group transformer.
   * @param stagingProcessor The staging processor.
   * @param mergeProcessor The merge processor.
   * @param disposeBatchProcessor The dispose batch processor.
   * @return The GenericStreamingGraphBuilder instance.
   */
  def apply(streamDataProvider: StreamDataProvider,
            fieldFilteringProcessor: FieldFilteringTransformer,
            groupTransformer: GenericGroupingTransformer,
            stagingProcessor: StagingProcessor,
            mergeProcessor: MergeBatchProcessor,
            disposeBatchProcessor: DisposeBatchProcessor): GenericStreamingGraphBuilder =
    new GenericStreamingGraphBuilder(streamDataProvider,
      fieldFilteringProcessor,
      groupTransformer,
      stagingProcessor,
      mergeProcessor,
      disposeBatchProcessor)

  /**
   * The ZLayer for the GenericStreamingGraphBuilder.
   * This layer is used to inject the GenericStreamingGraphBuilder into the DI container.
   */
  val layer: ZLayer[Environment, Nothing, GenericStreamingGraphBuilder] =
    ZLayer {
      for
        streamDataProvider <- ZIO.service[StreamDataProvider]
        fieldFilteringProcessor <- ZIO.service[FieldFilteringTransformer]
        groupTransformer <- ZIO.service[GenericGroupingTransformer]
        stagingProcessor <- ZIO.service[StagingProcessor]
        mergeProcessor <- ZIO.service[MergeBatchProcessor]
        disposeBatchProcessor <- ZIO.service[DisposeBatchProcessor]
      yield GenericStreamingGraphBuilder(streamDataProvider,
        fieldFilteringProcessor,
        groupTransformer,
        stagingProcessor,
        mergeProcessor,
        disposeBatchProcessor)
    }

  /**
   * The ZLayer for the GenericStreamingGraphBuilder.
   * This layer is used to inject the GenericStreamingGraphBuilder into the DI container as a BackfillSubStream
   * interface implementation. The `layer` cannot be used for this purpose because it injects the
   * GenericStreamingGraphBuilder as all the interfaces it implements, and the container cannot resolve the
   * correct implementation since backfill builders also implement the same interfaces.
   */
  val backfillSubStreamLayer: ZLayer[Environment, Nothing, BackfillSubStream] =
    ZLayer {
      for
        streamDataProvider <- ZIO.service[StreamDataProvider]
        fieldFilteringProcessor <- ZIO.service[FieldFilteringTransformer]
        groupTransformer <- ZIO.service[GenericGroupingTransformer]
        stagingProcessor <- ZIO.service[StagingProcessor]
        mergeProcessor <- ZIO.service[MergeBatchProcessor]
        disposeBatchProcessor <- ZIO.service[DisposeBatchProcessor]
      yield GenericStreamingGraphBuilder(streamDataProvider,
        fieldFilteringProcessor,
        groupTransformer,
        stagingProcessor,
        mergeProcessor,
        disposeBatchProcessor)
    }
