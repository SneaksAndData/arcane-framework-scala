package com.sneaksanddata.arcane.framework
package services.streaming.graph

import services.streaming.base.{StreamDataProvider, StreamingGraphBuilder}
import services.streaming.processors.batch_processors.maintenance.TargetMaintenanceProcessor
import services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor, SchemaMigrationProcessor, WatermarkProcessor}
import services.streaming.processors.transformers.{FieldFilteringTransformer, IngestionTimeAppender, StagingProcessor, TimeAppender}

import zio.stream.ZStream
import zio.{Tag, ZIO, ZLayer}

/** Provides the complete data stream for the streaming process including all the stages and services except the sink
  * and lifetime service.
  */
class DefaultStreamingGraphBuilder(
    streamDataProvider: StreamDataProvider,
    fieldFilteringProcessor: FieldFilteringTransformer,
    timeAppender: TimeAppender,
    stagingProcessor: StagingProcessor,
    mergeProcessor: MergeBatchProcessor,
    disposeBatchProcessor: DisposeBatchProcessor,
    watermarkProcessor: WatermarkProcessor,
    schemaMigrationProcessor: SchemaMigrationProcessor,
    targetMaintenanceProcessor: TargetMaintenanceProcessor
) extends StreamingGraphBuilder:

  /** @inheritdoc
    */
  override type ProcessedBatch = DisposeBatchProcessor#BatchType

  /** @inheritdoc
    */
  override def produce(): ZStream[Any, Throwable, ProcessedBatch] =
    streamDataProvider.stream.flatMap { case (subStream, schema) =>
      val outputSchema = timeAppender.appendToSchema(schema)

      subStream
        .via(fieldFilteringProcessor.process)
        .via(timeAppender.process)
        .via(stagingProcessor.process(outputSchema))
        .via(schemaMigrationProcessor.process)
        .via(mergeProcessor.process)
        .via(targetMaintenanceProcessor.process)
        .via(watermarkProcessor.process)
        .via(disposeBatchProcessor.process)
    }

object DefaultStreamingGraphBuilder:

  /** The environment required for the DefaultStreamingGraphBuilder.
    */
  type Environment = StreamDataProvider & FieldFilteringTransformer & StagingProcessor & MergeBatchProcessor &
    DisposeBatchProcessor & WatermarkProcessor & SchemaMigrationProcessor & TargetMaintenanceProcessor

  /** Creates a new DefaultStreamingGraphBuilder.
    */
  def apply(
      streamDataProvider: StreamDataProvider,
      fieldFilteringProcessor: FieldFilteringTransformer,
      timeAppender: TimeAppender,
      stagingProcessor: StagingProcessor,
      mergeProcessor: MergeBatchProcessor,
      disposeBatchProcessor: DisposeBatchProcessor,
      watermarkProcessor: WatermarkProcessor,
      schemaMigrationProcessor: SchemaMigrationProcessor,
      targetMaintenanceProcessor: TargetMaintenanceProcessor
  ): DefaultStreamingGraphBuilder =
    new DefaultStreamingGraphBuilder(
      streamDataProvider,
      fieldFilteringProcessor,
      timeAppender,
      stagingProcessor,
      mergeProcessor,
      disposeBatchProcessor,
      watermarkProcessor,
      schemaMigrationProcessor,
      targetMaintenanceProcessor
    )

  val layer: ZLayer[Environment, Nothing, DefaultStreamingGraphBuilder] =
    ZLayer {
      for
        streamDataProvider         <- ZIO.service[StreamDataProvider]
        fieldFilteringProcessor    <- ZIO.service[FieldFilteringTransformer]
        stagingProcessor           <- ZIO.service[StagingProcessor]
        mergeProcessor             <- ZIO.service[MergeBatchProcessor]
        disposeBatchProcessor      <- ZIO.service[DisposeBatchProcessor]
        watermarkProcessor         <- ZIO.service[WatermarkProcessor]
        schemaMigrationProcessor   <- ZIO.service[SchemaMigrationProcessor]
        targetMaintenanceProcessor <- ZIO.service[TargetMaintenanceProcessor]
      yield DefaultStreamingGraphBuilder(
        streamDataProvider,
        fieldFilteringProcessor,
        IngestionTimeAppender,
        stagingProcessor,
        mergeProcessor,
        disposeBatchProcessor,
        watermarkProcessor,
        schemaMigrationProcessor,
        targetMaintenanceProcessor
      )
    }
