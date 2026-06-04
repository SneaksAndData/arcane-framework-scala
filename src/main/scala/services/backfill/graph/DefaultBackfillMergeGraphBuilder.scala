package com.sneaksanddata.arcane.framework
package services.backfill.graph

import services.streaming.base.StreamDataProvider
import services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor, SchemaMigrationProcessor, WatermarkProcessor}
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}

import zio.stream.ZStream
import zio.{ZIO, ZLayer}

/**
 * Generates a stream graph for backfill MERGE mode.
  */
class DefaultBackfillMergeGraphBuilder(
    streamDataProvider: StreamDataProvider,
    fieldFilteringProcessor: FieldFilteringTransformer,
    stagingProcessor: StagingProcessor,
    mergeProcessor: MergeBatchProcessor,
    watermarkProcessor: WatermarkProcessor,
    schemaMigrationProcessor: SchemaMigrationProcessor,
) extends BackfillStreamingGraphBuilder:
  override type ProcessedBatch = DisposeBatchProcessor#BatchType

  override def produce(): ZStream[Any, Throwable, ProcessedBatch] = streamDataProvider.stream.flatMap { case (subStream, schema) =>
    subStream
      .via(fieldFilteringProcessor.process)
      .via(stagingProcessor.process(schema))
      .via(schemaMigrationProcessor.process)
      .via(mergeProcessor.process)
      .via(watermarkProcessor.process)
  }

object DefaultBackfillMergeGraphBuilder:

  type Environment = StreamDataProvider & FieldFilteringTransformer & StagingProcessor & MergeBatchProcessor & WatermarkProcessor & SchemaMigrationProcessor

  val layer: ZLayer[Environment, Nothing, DefaultBackfillMergeGraphBuilder] =
    ZLayer {
      for
        streamDataProvider         <- ZIO.service[StreamDataProvider]
        fieldFilteringProcessor    <- ZIO.service[FieldFilteringTransformer]
        stagingProcessor           <- ZIO.service[StagingProcessor]
        mergeProcessor             <- ZIO.service[MergeBatchProcessor]
        watermarkProcessor         <- ZIO.service[WatermarkProcessor]
        schemaMigrationProcessor   <- ZIO.service[SchemaMigrationProcessor]
      yield new DefaultBackfillMergeGraphBuilder(
        streamDataProvider,
        fieldFilteringProcessor,
        stagingProcessor,
        mergeProcessor,
        watermarkProcessor,
        schemaMigrationProcessor
      )
    }