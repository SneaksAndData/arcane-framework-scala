package com.sneaksanddata.arcane.framework
package tests.services.streaming

import models.*
import models.app.StreamContext
import models.batches.SqlServerChangeTrackingMergeBatch
import models.schemas.{ArcaneSchema, ArcaneType, DataCell, MergeKeyField}
import models.settings.{BufferingStrategy, SourceBufferingSettings}
import services.app.GenericStreamRunnerService
import services.app.base.StreamRunnerService
import services.base.{BatchOptimizationResult, DisposeServiceClient, MergeServiceClient}
import services.filters.FieldsFilteringService
import services.iceberg.{IcebergS3CatalogWriter, IcebergTablePropertyManager}
import services.merging.JdbcTableManager
import services.metrics.{ArcaneDimensionsProvider, DeclaredMetrics}
import services.streaming.base.{HookManager, StreamDataProvider}
import services.streaming.graph_builders.GenericStreamingGraphBuilder
import services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  WatermarkProcessor
}
import services.streaming.processors.transformers.FieldFilteringTransformer.Environment
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import tests.services.streaming.processors.utils.TestIndexedStagedBatches
import tests.shared.*
import tests.shared.IcebergCatalogInfo.*

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import org.easymock.EasyMock
import org.easymock.EasyMock.{replay, verify}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import org.scalatestplus.easymock.EasyMockSugar
import zio.stream.ZStream
import zio.{Runtime, Schedule, Unsafe, ZIO, ZLayer}

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
      streamDataProvider.stream.andReturn(ZStream.fromIterable(testInput).repeat(Schedule.forever))

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

      jdbcTableManager
        .cleanupStagingTables(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject())
        .andReturn(ZIO.unit)
        .anyTimes()
      jdbcTableManager.createTargetTable
        .andReturn(ZIO.unit)
        .anyTimes()
      jdbcTableManager.createBackFillTable
        .andReturn(ZIO.unit)
        .anyTimes()
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
        IcebergS3CatalogWriter.layer,

        // Settings
        ZLayer.succeed(TestStagingDataSettings),
        ZLayer.succeed(TablePropertiesSettings),
        ZLayer.succeed(TestSinkSettings),
        ZLayer.succeed(defaultStagingSettings),
        ZLayer.succeed(TestFieldSelectionRuleSettings),

        // Mocks
        ZLayer.succeed(new TestStreamLifetimeService(streamRepeatCount - 1, identity)),
        ZLayer.succeed(disposeServiceClient),
        ZLayer.succeed(mergeServiceClient),
        ZLayer.succeed(jdbcTableManager),
        ZLayer.succeed(hookManager),
        ZLayer.succeed(streamDataProvider),
        ZLayer.succeed(new StreamContext {
          override def IsBackfilling: Boolean = false
          override def streamId: String       = "test-stream-id"
          override def streamKind: String     = "test-stream-kind"
        }),
        ZLayer.succeed(TestSourceBufferingSettings),
        DeclaredMetrics.layer,
        ArcaneDimensionsProvider.layer,
        WatermarkProcessor.layer,
        IcebergTablePropertyManager.layer
      )

    // Act
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(streamRunnerService.flatMap(_.run))).map { _ =>
      // Assert
      noException should be thrownBy verify(streamDataProvider, hookManager)
    }
  }
