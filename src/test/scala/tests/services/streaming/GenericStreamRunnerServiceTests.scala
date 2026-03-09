package com.sneaksanddata.arcane.framework
package tests.services.streaming

import models.*
import models.app.{BaseStreamContext, PluginStreamContext}
import models.batches.SqlServerChangeTrackingMergeBatch
import models.schemas.{ArcaneSchema, ArcaneType, DataCell, MergeKeyField, given_CanAdd_ArcaneSchema}
import models.settings.sources.{BufferingStrategy, SourceBufferingSettings, StreamSourceSettings}
import services.app.GenericStreamRunnerService
import services.app.base.StreamRunnerService
import services.base.{BatchOptimizationResult, DisposeServiceClient, MergeServiceClient, SchemaProvider}
import services.filters.FieldsFilteringService
import services.iceberg.{IcebergEntityManager, IcebergS3CatalogWriter, IcebergStagingEntityManager, IcebergTablePropertyManager}
import services.merging.JdbcTableManager
import services.metrics.{ArcaneDimensionsProvider, DeclaredMetrics}
import services.streaming.base.{HookManager, StreamDataProvider}
import services.streaming.graph_builders.GenericStreamingGraphBuilder
import services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor, WatermarkProcessor}
import services.streaming.processors.transformers.FieldFilteringTransformer.Environment
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import tests.services.streaming.processors.utils.TestIndexedStagedBatches
import tests.shared.*
import tests.shared.IcebergCatalogInfo.*
import services.bootstrap.DefaultStreamBootstrapper

import com.sneaksanddata.arcane.framework.models.settings.observability.ObservabilitySettings
import com.sneaksanddata.arcane.framework.models.settings.sink.SinkSettings
import com.sneaksanddata.arcane.framework.models.settings.staging.StagingSettings
import com.sneaksanddata.arcane.framework.models.settings.streaming.{StreamModeSettings, ThroughputSettings}
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
    val jdbcTableManager     = mock[JdbcTableManager]
    val hookManager          = mock[HookManager]
    val streamDataProvider   = mock[StreamDataProvider]

    expecting {

      // The data provider mock provides an infinite stream of test input
      streamDataProvider.stream.andReturn(ZStream.fromIterable(testInput).repeat(Schedule.forever).rechunk(1))

      // The hookManager.onStagingTablesComplete method is called ``streamRepeatCount`` times
      // It produces the empty set of staged batches, so the rest  of the pipeline can continue
      // but no further stages being invoked
      hookManager
        .onStagingTablesComplete(EasyMock.anyObject(), EasyMock.anyLong(), EasyMock.anyObject())
        .andReturn(new TestIndexedStagedBatches(List.empty, 0))
        .times(streamRepeatCount)
      hookManager
        .onBatchStaged(
          EasyMock.anyObject(),
          EasyMock.anyString(),
          EasyMock.anyString(),
          EasyMock.anyObject(),
          EasyMock.anyString(),
          EasyMock.anyObject(),
          EasyMock.anyObject()
        )
        .andReturn(
          SqlServerChangeTrackingMergeBatch(
            "test",
            ArcaneSchema(Seq(MergeKeyField)),
            "test",
            TablePropertiesSettings,
            None
          )
        )
        .times(streamRepeatCount)

      jdbcTableManager.optimizeTable(None).andReturn(ZIO.succeed(BatchOptimizationResult(false))).anyTimes()
      jdbcTableManager.expireSnapshots(None).andReturn(ZIO.succeed(BatchOptimizationResult(false))).anyTimes()
      jdbcTableManager.expireOrphanFiles(None).andReturn(ZIO.succeed(BatchOptimizationResult(false))).anyTimes()
      jdbcTableManager.analyzeTable(None).andReturn(ZIO.succeed(BatchOptimizationResult(false))).anyTimes()
    }
    replay(streamDataProvider, hookManager, jdbcTableManager)

    val streamRunnerService = ZIO
      .service[StreamRunnerService]
      .provide(
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

        // Settings
        ZLayer.succeed(TestStagingTableSettings),
        ZLayer.succeed(TablePropertiesSettings),
        ZLayer.succeed(TestSinkSettings),
        ZLayer.succeed(defaultIcebergStagingSettings),
        ZLayer.succeed(TestFieldSelectionRuleSettings),

        // Mocks
        ZLayer.succeed(new TestStreamLifetimeService(streamRepeatCount, identity)),
        ZLayer.succeed(disposeServiceClient),
        ZLayer.succeed(mergeServiceClient),
        ZLayer.succeed(jdbcTableManager),
        ZLayer.succeed(hookManager),
        ZLayer.succeed(new SchemaProvider[ArcaneSchema] {
          override type SchemaType = ArcaneSchema
          override def getSchema: Task[SchemaType] = ZIO.succeed(Seq(MergeKeyField))

          override def empty: SchemaType = ArcaneSchema.empty()
        }),
        ZLayer.succeed(streamDataProvider),
        ZLayer.succeed(new PluginStreamContext {
          override def IsBackfilling: Boolean = false
          override def streamId: String       = "test-stream-id"
          override def streamKind: String     = "test-stream-kind"

          override val streamMode: StreamModeSettings = ???
          override val source: StreamSourceSettings = ???
          override val sink: SinkSettings = ???
          override val observability: ObservabilitySettings = ???
          override val staging: StagingSettings = ???
          override val throughput: ThroughputSettings = ???
        }),
        ZLayer.succeed(TestBackfillTableSettings),
        ZLayer.succeed(TestSourceBufferingSettings),
        DeclaredMetrics.layer,
        ArcaneDimensionsProvider.layer,
        WatermarkProcessor.layer,
        IcebergTablePropertyManager.sinkLayer,
        // TODO: not used yet IcebergTablePropertyManager.stagingLayer,
        DefaultStreamBootstrapper.layer
      )

    // Act
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(streamRunnerService.flatMap(_.run))).map { _ =>
      // Assert
      noException should be thrownBy verify(streamDataProvider, hookManager)
    }
  }
