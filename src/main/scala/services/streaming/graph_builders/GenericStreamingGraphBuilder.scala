package com.sneaksanddata.arcane.framework
package services.streaming.graph_builders

import models.app.PluginStreamContext
import services.app.base.StreamLifetimeService
import services.streaming.base.{StreamDataProvider, StreamingGraphBuilder}
import services.streaming.processors.batch_processors.maintenance.TargetMaintenanceProcessor
import services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  SchemaMigrationProcessor,
  WatermarkProcessor
}
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import com.sneaksanddata.arcane.framework.services.backfill.BackfillSubStream

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
    watermarkProcessor: WatermarkProcessor,
    schemaMigrationProcessor: SchemaMigrationProcessor,
    targetMaintenanceProcessor: TargetMaintenanceProcessor
) extends StreamingGraphBuilder
    with BackfillSubStream:

  /** @inheritdoc
    */
  override type ProcessedBatch = DisposeBatchProcessor#BatchType

  /** @inheritdoc
    */
  override def produce(): ZStream[Any, Throwable, ProcessedBatch] =
    streamDataProvider.stream.flatMap { case (subStream, schema) =>
      subStream
        .via(fieldFilteringProcessor.process)
        .via(stagingProcessor.process(schema))
        .via(schemaMigrationProcessor.process)
        .via(mergeProcessor.process)
        .via(targetMaintenanceProcessor.process)
        .via(watermarkProcessor.process)
        .via(disposeBatchProcessor.process)
    }

object GenericStreamingGraphBuilder:

  /** The environment required for the GenericStreamingGraphBuilder.
    */
  type Environment = StreamDataProvider & FieldFilteringTransformer & StagingProcessor & MergeBatchProcessor &
    DisposeBatchProcessor & StreamLifetimeService & WatermarkProcessor & PluginStreamContext &
    SchemaMigrationProcessor & TargetMaintenanceProcessor

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
      watermarkProcessor: WatermarkProcessor,
      schemaMigrationProcessor: SchemaMigrationProcessor,
      targetMaintenanceProcessor: TargetMaintenanceProcessor
  ): GenericStreamingGraphBuilder =
    new GenericStreamingGraphBuilder(
      streamDataProvider,
      fieldFilteringProcessor,
      stagingProcessor,
      mergeProcessor,
      disposeBatchProcessor,
      watermarkProcessor,
      schemaMigrationProcessor,
      targetMaintenanceProcessor
    )

  /** The ZLayer for the GenericStreamingGraphBuilder. This layer is used to inject the GenericStreamingGraphBuilder
    * into the DI container.
    */
  val layer: ZLayer[Environment, Nothing, GenericStreamingGraphBuilder] =
    ZLayer {
      for
        context                    <- ZIO.service[PluginStreamContext]
        streamDataProvider         <- ZIO.service[StreamDataProvider]
        fieldFilteringProcessor    <- ZIO.service[FieldFilteringTransformer]
        stagingProcessor           <- ZIO.service[StagingProcessor]
        mergeProcessor             <- ZIO.service[MergeBatchProcessor]
        disposeBatchProcessor      <- ZIO.service[DisposeBatchProcessor]
        watermarkProcessor         <- ZIO.service[WatermarkProcessor]
        schemaMigrationProcessor   <- ZIO.service[SchemaMigrationProcessor]
        targetMaintenanceProcessor <- ZIO.service[TargetMaintenanceProcessor]
      yield GenericStreamingGraphBuilder(
        streamDataProvider,
        fieldFilteringProcessor,
        stagingProcessor,
        mergeProcessor,
        disposeBatchProcessor,
        watermarkProcessor,
        schemaMigrationProcessor,
        targetMaintenanceProcessor
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
        context                    <- ZIO.service[PluginStreamContext]
        streamDataProvider         <- ZIO.service[StreamDataProvider]
        fieldFilteringProcessor    <- ZIO.service[FieldFilteringTransformer]
        stagingProcessor           <- ZIO.service[StagingProcessor]
        mergeProcessor             <- ZIO.service[MergeBatchProcessor]
        disposeBatchProcessor      <- ZIO.service[DisposeBatchProcessor]
        watermarkProcessor         <- ZIO.service[WatermarkProcessor]
        schemaMigrationProcessor   <- ZIO.service[SchemaMigrationProcessor]
        targetMaintenanceProcessor <- ZIO.service[TargetMaintenanceProcessor]
      yield GenericStreamingGraphBuilder(
        streamDataProvider,
        fieldFilteringProcessor,
        stagingProcessor,
        mergeProcessor,
        disposeBatchProcessor,
        watermarkProcessor,
        schemaMigrationProcessor,
        targetMaintenanceProcessor
      )
    }
