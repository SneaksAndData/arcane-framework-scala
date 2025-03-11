package com.sneaksanddata.arcane.framework
package services.streaming.graph_builders.base

import models.{DataRow, given_MetadataEnrichedRowStreamElement_DataRow}
import services.app.base.StreamLifetimeService
import services.streaming.base.{HookManager, MetadataEnrichedRowStreamElement, StreamDataProvider, StreamingGraphBuilder}
import services.streaming.processors.GenericGroupingTransformer
import services.streaming.processors.batch_processors.{DisposeBatchProcessor, MergeBatchProcessor}
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}

import zio.stream.ZStream
import zio.{Tag, ZIO, ZLayer}

class GenericStreamingGraphBuilder(streamDataProvider: StreamDataProvider,
                                   fieldFilteringProcessor: FieldFilteringTransformer,
                                   groupTransformer: GenericGroupingTransformer,
                                   stagingProcessor: StagingProcessor,
                                   mergeProcessor: MergeBatchProcessor,
                                   disposeBatchProcessor: DisposeBatchProcessor,
                                   hookManager: HookManager)
  extends StreamingGraphBuilder:

  type ProcessedBatch = DisposeBatchProcessor#BatchType

  override def produce: ZStream[Any, Throwable, ProcessedBatch] =
    streamDataProvider.stream
      .via(fieldFilteringProcessor.process)
      .via(groupTransformer.process)
      .via(stagingProcessor.process(hookManager.onStagingTablesComplete))
      .via(mergeProcessor.process)
      .via(disposeBatchProcessor.process)

object GenericStreamingGraphBuilder:

  type Environment = StreamDataProvider
    & GenericGroupingTransformer
    & FieldFilteringTransformer
    & StagingProcessor
    & MergeBatchProcessor
    & DisposeBatchProcessor
    & StreamLifetimeService
    & HookManager


  def apply[T: MetadataEnrichedRowStreamElement: Tag](streamDataProvider: StreamDataProvider,
                                                      fieldFilteringProcessor: FieldFilteringTransformer,
                                                      groupTransformer: GenericGroupingTransformer,
                                                      stagingProcessor: StagingProcessor,
                                                      mergeProcessor: MergeBatchProcessor,
                                                      disposeBatchProcessor: DisposeBatchProcessor,
                                                      hookManager: HookManager): GenericStreamingGraphBuilder =
    new GenericStreamingGraphBuilder(streamDataProvider,
      fieldFilteringProcessor,
      groupTransformer,
      stagingProcessor,
      mergeProcessor,
      disposeBatchProcessor,
      hookManager)

  def layer: ZLayer[Environment, Nothing, GenericStreamingGraphBuilder] =
    ZLayer {
      for
        streamDataProvider <- ZIO.service[StreamDataProvider]
        fieldFilteringProcessor <- ZIO.service[FieldFilteringTransformer]
        groupTransformer <- ZIO.service[GenericGroupingTransformer]
        stagingProcessor <- ZIO.service[StagingProcessor]
        mergeProcessor <- ZIO.service[MergeBatchProcessor]
        disposeBatchProcessor <- ZIO.service[DisposeBatchProcessor]
        hookManager <- ZIO.service[HookManager]
      yield GenericStreamingGraphBuilder(streamDataProvider,
        fieldFilteringProcessor,
        groupTransformer,
        stagingProcessor,
        mergeProcessor,
        disposeBatchProcessor,
        hookManager)
    }
