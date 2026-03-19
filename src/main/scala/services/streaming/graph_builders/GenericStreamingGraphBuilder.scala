package com.sneaksanddata.arcane.framework
package services.streaming.graph_builders

import logging.ZIOLogAnnotations.zlogStream
import models.app.PluginStreamContext
import models.schemas.DataCell
import models.settings.sources.{BufferingImpl, SourceBufferingSettings, UnboundedImpl}
import services.app.base.StreamLifetimeService
import services.streaming.base.{BackfillSubStream, HookManager, StreamDataProvider, StreamingGraphBuilder}
import services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  WatermarkProcessor
}
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}

import zio.stream.ZStream
import zio.{Tag, ZIO, ZLayer}

/** Provides the complete data stream for the streaming process including all the stages and services except the sink
  * and lifetime service.
  */
class GenericStreamingGraphBuilder(
    streamDataProvider: StreamDataProvider,
    fieldFilteringProcessor: FieldFilteringTransformer,
    stagingProcessor: StagingProcessor,
    mergeProcessor: MergeBatchProcessor,
    disposeBatchProcessor: DisposeBatchProcessor,
    watermarkProcessor: WatermarkProcessor
) extends StreamingGraphBuilder
    with BackfillSubStream:

  /** @inheritdoc
    */
  override type ProcessedBatch = DisposeBatchProcessor#BatchType

  /** @inheritdoc
    */
  override def produce(hookManager: HookManager): ZStream[Any, Throwable, ProcessedBatch] =
    streamDataProvider.stream
      .via(fieldFilteringProcessor.process)
      .via(stagingProcessor.process(hookManager.onStagingTablesComplete, hookManager.onBatchStaged))
      .via(mergeProcessor.process)
      .via(watermarkProcessor.process)
      .via(disposeBatchProcessor.process)

object GenericStreamingGraphBuilder:

  /** The environment required for the GenericStreamingGraphBuilder.
    */
  type Environment = StreamDataProvider & FieldFilteringTransformer & StagingProcessor & MergeBatchProcessor &
    DisposeBatchProcessor & StreamLifetimeService & WatermarkProcessor & PluginStreamContext

  /** Creates a new GenericStreamingGraphBuilder.
    * @param streamDataProvider
    *   The stream data provider.
    * @param fieldFilteringProcessor
    *   The field filtering processor.
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
      stagingProcessor: StagingProcessor,
      mergeProcessor: MergeBatchProcessor,
      disposeBatchProcessor: DisposeBatchProcessor,
      watermarkProcessor: WatermarkProcessor
  ): GenericStreamingGraphBuilder =
    new GenericStreamingGraphBuilder(
      streamDataProvider,
      fieldFilteringProcessor,
      stagingProcessor,
      mergeProcessor,
      disposeBatchProcessor,
      watermarkProcessor
    )

  /** The ZLayer for the GenericStreamingGraphBuilder. This layer is used to inject the GenericStreamingGraphBuilder
    * into the DI container.
    */
  val layer: ZLayer[Environment, Nothing, GenericStreamingGraphBuilder] =
    ZLayer {
      for
        context                 <- ZIO.service[PluginStreamContext]
        streamDataProvider      <- ZIO.service[StreamDataProvider]
        fieldFilteringProcessor <- ZIO.service[FieldFilteringTransformer]
        stagingProcessor        <- ZIO.service[StagingProcessor]
        mergeProcessor          <- ZIO.service[MergeBatchProcessor]
        disposeBatchProcessor   <- ZIO.service[DisposeBatchProcessor]
        watermarkProcessor      <- ZIO.service[WatermarkProcessor]
      yield GenericStreamingGraphBuilder(
        streamDataProvider,
        fieldFilteringProcessor,
        stagingProcessor,
        mergeProcessor,
        disposeBatchProcessor,
        watermarkProcessor
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
        context                 <- ZIO.service[PluginStreamContext]
        streamDataProvider      <- ZIO.service[StreamDataProvider]
        fieldFilteringProcessor <- ZIO.service[FieldFilteringTransformer]
        stagingProcessor        <- ZIO.service[StagingProcessor]
        mergeProcessor          <- ZIO.service[MergeBatchProcessor]
        disposeBatchProcessor   <- ZIO.service[DisposeBatchProcessor]
        watermarkProcessor      <- ZIO.service[WatermarkProcessor]
      yield GenericStreamingGraphBuilder(
        streamDataProvider,
        fieldFilteringProcessor,
        stagingProcessor,
        mergeProcessor,
        disposeBatchProcessor,
        watermarkProcessor
      )
    }
