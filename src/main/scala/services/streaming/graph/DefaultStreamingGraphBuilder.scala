package com.sneaksanddata.arcane.framework
package services.streaming.graph

import services.streaming.base.{StreamDataProvider, StreamingGraphBuilder}
import services.streaming.processors.batch_processors.maintenance.TargetMaintenanceProcessor
import services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  SchemaMigrationProcessor,
  WatermarkProcessor
}
import services.streaming.processors.transformers.StagingProcessor

import zio.stream.ZStream
import zio.{Tag, ZIO, ZLayer}

/** Provides the complete data stream for the streaming process including all the stages and services except the sink
  * and lifetime service.
  */
class DefaultStreamingGraphBuilder(
    streamDataProvider: StreamDataProvider,
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
      subStream
        .via(stagingProcessor.process(schema))
        .via(schemaMigrationProcessor.process)
        .via(mergeProcessor.process)
        .via(targetMaintenanceProcessor.process)
        .via(watermarkProcessor.process)
        .via(disposeBatchProcessor.process)
    }

object DefaultStreamingGraphBuilder:

  /** The environment required for the DefaultStreamingGraphBuilder.
    */
  type Environment = StreamDataProvider & StagingProcessor & MergeBatchProcessor &
    DisposeBatchProcessor & WatermarkProcessor & SchemaMigrationProcessor & TargetMaintenanceProcessor

  /** Creates a new DefaultStreamingGraphBuilder.
    */
  def apply(
      streamDataProvider: StreamDataProvider,
      stagingProcessor: StagingProcessor,
      mergeProcessor: MergeBatchProcessor,
      disposeBatchProcessor: DisposeBatchProcessor,
      watermarkProcessor: WatermarkProcessor,
      schemaMigrationProcessor: SchemaMigrationProcessor,
      targetMaintenanceProcessor: TargetMaintenanceProcessor
  ): DefaultStreamingGraphBuilder =
    new DefaultStreamingGraphBuilder(
      streamDataProvider,
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
        stagingProcessor           <- ZIO.service[StagingProcessor]
        mergeProcessor             <- ZIO.service[MergeBatchProcessor]
        disposeBatchProcessor      <- ZIO.service[DisposeBatchProcessor]
        watermarkProcessor         <- ZIO.service[WatermarkProcessor]
        schemaMigrationProcessor   <- ZIO.service[SchemaMigrationProcessor]
        targetMaintenanceProcessor <- ZIO.service[TargetMaintenanceProcessor]
      yield DefaultStreamingGraphBuilder(
        streamDataProvider,
        stagingProcessor,
        mergeProcessor,
        disposeBatchProcessor,
        watermarkProcessor,
        schemaMigrationProcessor,
        targetMaintenanceProcessor
      )
    }
