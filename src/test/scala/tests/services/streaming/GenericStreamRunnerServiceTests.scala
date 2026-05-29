package com.sneaksanddata.arcane.framework
package tests.services.streaming

import models.*
import models.schemas.{
  ArcaneSchema,
  ArcaneType,
  DataCell,
  IndexedField,
  IndexedMergeKeyField,
  MergeKeyField,
  given_CanAdd_ArcaneSchema
}
import models.schemas.ArcaneType.StringType
import services.app.GenericStreamRunnerService
import services.app.base.StreamRunnerService
import services.base.{BatchDisposeResult, DisposeServiceClient, MergeServiceClient, SchemaProvider, StreamingSource}
import services.filters.FieldsFilteringService
import services.iceberg.{
  IcebergEntityManager,
  IcebergS3CatalogWriter,
  IcebergStagingEntityManager,
  IcebergTablePropertyManager
}
import services.metrics.{DeclaredMetrics, GlobalMetricTagProvider}
import services.streaming.base.StreamDataProvider
import services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  SchemaMigrationProcessor,
  WatermarkProcessor
}
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import services.streaming.processors.batch_processors.maintenance.TargetMaintenanceProcessor
import services.bootstrap.DefaultStreamBootstrapper
import services.streaming.graph.DefaultStreamingGraphBuilder
import tests.shared.*

import org.easymock.EasyMock
import org.easymock.EasyMock.{replay, verify}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.easymock.EasyMockSugar
import zio.stream.ZStream
import zio.{Runtime, Schedule, Task, Unsafe, ZIO, ZLayer}

class GenericStreamRunnerServiceTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
  private val runtime = Runtime.default
  private val testInput = List(
    List(
      DataCell("name", ArcaneType.StringType, "John Doe"),
      DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")
    ),
    List(
      DataCell("name", ArcaneType.StringType, "John"),
      DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")
    )
  )

  it should "gracefully handle stream shutdown" in {
    // Arrange
    val streamRepeatCount = 5

    val disposeServiceClient = mock[DisposeServiceClient]
    val mergeServiceClient   = mock[MergeServiceClient]
    val streamDataProvider   = mock[StreamDataProvider]

    expecting {

      // The data provider mock provides an infinite stream of test input
      streamDataProvider.stream.andReturn(
        ZStream.succeed(
          (
            ZStream.fromIterable(testInput).repeat(Schedule.forever).rechunk(1),
            ArcaneSchema(Seq(IndexedMergeKeyField(1), IndexedField("name", StringType, 2)))
          )
        )
      )

      mergeServiceClient.applyBatch(EasyMock.anyObject()).andReturn(ZIO.succeed(true)).times(5)
      disposeServiceClient.disposeBatch(EasyMock.anyObject()).andReturn(ZIO.succeed(BatchDisposeResult(true))).times(5)
    }
    replay(streamDataProvider, mergeServiceClient, disposeServiceClient)

    val streamRunnerService = ZLayer.make[StreamRunnerService](
      // Real services
      GenericStreamRunnerService.layer,
      DefaultStreamingGraphBuilder.layer,
      DisposeBatchProcessor.layer,
      FieldFilteringTransformer.layer,
      MergeBatchProcessor.layer,
      StagingProcessor.layer,
      FieldsFilteringService.layer,
      IcebergEntityManager.sinkLayer,
      IcebergEntityManager.stagingLayer,
      IcebergS3CatalogWriter.layer,

      // Mocks
      ZLayer.succeed(new TestStreamLifetimeService(streamRepeatCount, identity)),
      ZLayer.succeed(disposeServiceClient),
      ZLayer.succeed(mergeServiceClient),
      ZLayer.succeed(new StreamingSource {
        override type SchemaType = ArcaneSchema
        override def getSchema: Task[SchemaType] = ZIO.succeed(Seq(MergeKeyField))

        override def deleteShards(streamId: String): Task[Unit] = ZIO.unit

        override def getShards: ZStream[Any, Throwable, ShardMetadata] = ZStream.empty

        override def empty: SchemaType = ArcaneSchema.empty()
      }),
      ZLayer.succeed(new TestStagedBatchFactory()),
      VoidSchemaMigrationProcessor.layer,
      ZLayer.succeed(streamDataProvider),
      ZLayer.succeed(TestPluginStreamContext),
      DeclaredMetrics.layer,
      TargetMaintenanceProcessor.layer,
      GlobalMetricTagProvider.layer,
      WatermarkProcessor.layer,
      IcebergTablePropertyManager.sinkLayer,
      IcebergTablePropertyManager.stagingLayer,
      DefaultStreamBootstrapper.layer
    )

    // Act
    Unsafe
      .unsafe(implicit unsafe =>
        runtime.unsafe.runToFuture(ZIO.service[StreamRunnerService].flatMap(_.run).provideLayer(streamRunnerService))
      )
      .map { _ =>
        // Assert
        noException should be thrownBy verify(streamDataProvider)
      }
  }
