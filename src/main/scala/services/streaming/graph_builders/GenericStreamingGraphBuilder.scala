package com.sneaksanddata.arcane.framework
package services.streaming.graph_builders

import logging.ZIOLogAnnotations.zlogStream
import models.schemas.DataCell
import models.settings.{BufferingStrategy, SourceBufferingSettings}
import services.app.base.StreamLifetimeService
import services.streaming.base.{BackfillSubStream, HookManager, StreamDataProvider, StreamingGraphBuilder}
import services.streaming.graph_builders.GenericStreamingGraphBuilder.trySetBuffering
import services.streaming.processors.GenericGroupingTransformer
import services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor}
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}

import zio.stream.ZStream
import zio.{Tag, ZIO, ZLayer}

/** Provides the complete data stream for the streaming process including all the stages and services except the sink
  * and lifetime service.
  */
class GenericStreamingGraphBuilder(
    streamDataProvider: StreamDataProvider,
    fieldFilteringProcessor: FieldFilteringTransformer,
    groupTransformer: GenericGroupingTransformer,
    stagingProcessor: StagingProcessor,
    mergeProcessor: MergeBatchProcessor,
    disposeBatchProcessor: DisposeBatchProcessor,
    sourceBufferingSettings: SourceBufferingSettings
) extends StreamingGraphBuilder
    with BackfillSubStream:

  /** @inheritdoc
    */
  override type ProcessedBatch = DisposeBatchProcessor#BatchType

  /** @inheritdoc
    */
  override def produce(hookManager: HookManager): ZStream[Any, Throwable, ProcessedBatch] =
    streamDataProvider.stream
      .trySetBuffering(sourceBufferingSettings)
      .via(fieldFilteringProcessor.process)
      .via(groupTransformer.process)
      .via(stagingProcessor.process(hookManager.onStagingTablesComplete, hookManager.onBatchStaged))
      .via(mergeProcessor.process)
      .via(disposeBatchProcessor.process)

object GenericStreamingGraphBuilder:

  extension (stream: ZStream[Any, Throwable, List[DataCell]])
    /** Configures the upstream to buffer the data in memory.
      * @return
      *   The ZStream of DisposeBatchProcessor#BatchType.
      */
    def trySetBuffering(settings: SourceBufferingSettings): ZStream[Any, Throwable, List[DataCell]] =
      (settings.bufferingEnabled, settings.bufferingStrategy) match
        case (true, BufferingStrategy.Unbounded) =>
          zlogStream("Running stream with unbound source buffer") *> stream.bufferUnbounded

        case (true, BufferingStrategy.Buffering(size)) =>
          zlogStream("Running stream with bound source buffer size %s", size.toString) *> stream.buffer(size)

        case (false, _) => zlogStream("Running stream with disabled source buffering") *> stream

  /** The environment required for the GenericStreamingGraphBuilder.
    */
  type Environment = StreamDataProvider & GenericGroupingTransformer & FieldFilteringTransformer & StagingProcessor &
    MergeBatchProcessor & DisposeBatchProcessor & StreamLifetimeService & SourceBufferingSettings

  /** Creates a new GenericStreamingGraphBuilder.
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
    * @return
    *   The GenericStreamingGraphBuilder instance.
    */
  def apply(
      streamDataProvider: StreamDataProvider,
      fieldFilteringProcessor: FieldFilteringTransformer,
      groupTransformer: GenericGroupingTransformer,
      stagingProcessor: StagingProcessor,
      mergeProcessor: MergeBatchProcessor,
      disposeBatchProcessor: DisposeBatchProcessor,
      sourceBufferingSettings: SourceBufferingSettings
  ): GenericStreamingGraphBuilder =
    new GenericStreamingGraphBuilder(
      streamDataProvider,
      fieldFilteringProcessor,
      groupTransformer,
      stagingProcessor,
      mergeProcessor,
      disposeBatchProcessor,
      sourceBufferingSettings
    )

  /** The ZLayer for the GenericStreamingGraphBuilder. This layer is used to inject the GenericStreamingGraphBuilder
    * into the DI container.
    */
  val layer: ZLayer[Environment, Nothing, GenericStreamingGraphBuilder] =
    ZLayer {
      for
        streamDataProvider      <- ZIO.service[StreamDataProvider]
        fieldFilteringProcessor <- ZIO.service[FieldFilteringTransformer]
        groupTransformer        <- ZIO.service[GenericGroupingTransformer]
        stagingProcessor        <- ZIO.service[StagingProcessor]
        mergeProcessor          <- ZIO.service[MergeBatchProcessor]
        disposeBatchProcessor   <- ZIO.service[DisposeBatchProcessor]
        sourceBufferingSettings <- ZIO.service[SourceBufferingSettings]
      yield GenericStreamingGraphBuilder(
        streamDataProvider,
        fieldFilteringProcessor,
        groupTransformer,
        stagingProcessor,
        mergeProcessor,
        disposeBatchProcessor,
        sourceBufferingSettings
      )
    }

  /** The ZLayer for the GenericStreamingGraphBuilder. This layer is used to inject the GenericStreamingGraphBuilder
    * into the DI container as a BackfillSubStream interface implementation. The `layer` cannot be used for this purpose
    * because it injects the GenericStreamingGraphBuilder as all the interfaces it implements, and the container cannot
    * resolve the correct implementation since backfill builders also implement the same interfaces.
    */
  val backfillSubStreamLayer: ZLayer[Environment, Nothing, BackfillSubStream] =
    ZLayer {
      for
        streamDataProvider      <- ZIO.service[StreamDataProvider]
        fieldFilteringProcessor <- ZIO.service[FieldFilteringTransformer]
        groupTransformer        <- ZIO.service[GenericGroupingTransformer]
        stagingProcessor        <- ZIO.service[StagingProcessor]
        mergeProcessor          <- ZIO.service[MergeBatchProcessor]
        disposeBatchProcessor   <- ZIO.service[DisposeBatchProcessor]
        sourceBufferingSettings <- ZIO.service[SourceBufferingSettings]
      yield GenericStreamingGraphBuilder(
        streamDataProvider,
        fieldFilteringProcessor,
        groupTransformer,
        stagingProcessor,
        mergeProcessor,
        disposeBatchProcessor,
        sourceBufferingSettings
      )
    }
