package com.sneaksanddata.arcane.framework
package tests.services.streaming

import models.*
import models.app.PluginStreamContext
import models.batches.SqlServerChangeTrackingMergeBatch
import models.schemas.{ArcaneSchema, ArcaneType, DataCell, MergeKeyField, given_CanAdd_ArcaneSchema}
import services.app.GenericStreamRunnerService
import services.app.base.StreamRunnerService
import services.base.{DisposeServiceClient, MergeServiceClient, SchemaProvider}
import services.filters.FieldsFilteringService
import services.iceberg.{
  IcebergEntityManager,
  IcebergS3CatalogWriter,
  IcebergStagingEntityManager,
  IcebergTablePropertyManager
}
import services.metrics.{DeclaredMetrics, GlobalMetricTagProvider}
import services.streaming.base.StreamDataProvider
import services.streaming.graph_builders.GenericStreamingGraphBuilder
import services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  WatermarkProcessor
}
import services.streaming.processors.transformers.FieldFilteringTransformer.Environment
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import tests.shared.*
import services.bootstrap.DefaultStreamBootstrapper

import org.easymock.EasyMock
import org.easymock.EasyMock.{replay, verify}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
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
      DataCell("family_name", ArcaneType.StringType, "Doe"),
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
      streamDataProvider.stream.andReturn(ZStream.fromIterable(testInput).repeat(Schedule.forever).rechunk(1))

      // The hookManager.onStagingTablesComplete method is called ``streamRepeatCount`` times
      // It produces the empty set of staged batches, so the rest  of the pipeline can continue
      // but no further stages being invoked
    }
    replay(streamDataProvider)

    val streamRunnerService = ZLayer.make[StreamRunnerService](
      // Real services
      GenericStreamRunnerService.layer,
      GenericStreamingGraphBuilder.layer,
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
      ZLayer.succeed(new SchemaProvider[ArcaneSchema] {
        override type SchemaType = ArcaneSchema
        override def getSchema: Task[SchemaType] = ZIO.succeed(Seq(MergeKeyField))

        override def empty: SchemaType = ArcaneSchema.empty()
      }),
      ZLayer.succeed(streamDataProvider),
      ZLayer.succeed(TestPluginStreamContext),
      DeclaredMetrics.layer,
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
