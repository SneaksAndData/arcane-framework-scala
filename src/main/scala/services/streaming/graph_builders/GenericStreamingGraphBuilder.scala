package com.sneaksanddata.arcane.framework
package services.streaming.graph_builders

import models.DataCell
import models.settings.GroupingSettings
import services.app.base.StreamLifetimeService
import services.streaming.base.{BackfillSubStream, HookManager, StreamDataProvider, StreamingGraphBuilder}
import services.streaming.graph_builders.GenericStreamingGraphBuilder.trySetBuffering
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
                                   disposeBatchProcessor: DisposeBatchProcessor,
                                   groupingSettings: GroupingSettings)
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
      .trySetBuffering(groupingSettings)
      .via(fieldFilteringProcessor.process)
      .via(groupTransformer.process)
      .via(stagingProcessor.process(hookManager.onStagingTablesComplete, hookManager.onBatchStaged))
      .via(mergeProcessor.process)
      .via(disposeBatchProcessor.process)

object GenericStreamingGraphBuilder:

  extension (stream: ZStream[Any, Throwable, List[DataCell]])
    /**
     * Configures the upstream to buffer the data in memory.
     * @return The ZStream of DisposeBatchProcessor#BatchType.
     */
    def trySetBuffering(groupingSettings: GroupingSettings): ZStream[Any, Throwable, List[DataCell]] =
      stream.buffer(groupingSettings.rowsPerGroup)

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
    & GroupingSettings


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
            disposeBatchProcessor: DisposeBatchProcessor,
            groupingSettings: GroupingSettings): GenericStreamingGraphBuilder =
    new GenericStreamingGraphBuilder(streamDataProvider,
      fieldFilteringProcessor,
      groupTransformer,
      stagingProcessor,
      mergeProcessor,
      disposeBatchProcessor,
      groupingSettings)

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
        groupingSettings <- ZIO.service[GroupingSettings]
      yield GenericStreamingGraphBuilder(streamDataProvider,
        fieldFilteringProcessor,
        groupTransformer,
        stagingProcessor,
        mergeProcessor,
        disposeBatchProcessor,
        groupingSettings)
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
        groupingSettings <- ZIO.service[GroupingSettings]
      yield GenericStreamingGraphBuilder(streamDataProvider,
        fieldFilteringProcessor,
        groupTransformer,
        stagingProcessor,
        mergeProcessor,
        disposeBatchProcessor,
        groupingSettings)
    }
