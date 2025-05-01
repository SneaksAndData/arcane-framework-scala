package com.sneaksanddata.arcane.framework
package services.streaming.data_providers.backfill

import models.*
import models.app.StreamContext
import services.base.{BatchOptimizationResult, DisposeServiceClient, MergeServiceClient}
import services.consumers.{SqlServerChangeTrackingMergeBatch, StagedBackfillOverwriteBatch, SynapseLinkBackfillOverwriteBatch}
import services.filters.FieldsFilteringService
import services.lakehouse.base.{CatalogWriter, IcebergCatalogSettings, S3CatalogFileIO}
import services.merging.JdbcTableManager
import services.streaming.base.{HookManager, StreamDataProvider, StreamingGraphBuilder}
import services.streaming.graph_builders.GenericStreamingGraphBuilder
import services.streaming.processors.GenericGroupingTransformer
import services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor}
import services.streaming.processors.transformers.FieldFilteringTransformer.Environment
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import services.streaming.processors.utils.TestIndexedStagedBatches
import tests.shared.IcebergCatalogInfo.*
import utils.*
import services.lakehouse.{IcebergCatalogCredential, IcebergS3CatalogWriter}

import com.sneaksanddata.arcane.framework.services.metrics.{ArcaneDimensionsProvider, DeclaredMetrics}

import com.sneaksanddata.arcane.framework.models.settings.{BufferingStrategy, SourceBufferingSettings}
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import org.easymock.EasyMock
import org.easymock.EasyMock.{replay, verify}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.{should, shouldBe}
import org.scalatestplus.easymock.EasyMockSugar
import zio.stream.ZStream
import zio.{Chunk, Runtime, Schedule, Task, Unsafe, ZIO, ZLayer}

class GenericBackfillStreamingMergeDataProviderTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
  private val runtime = Runtime.default

  it should "produce backfill batch if stream is completed" in {
    // Arrange
    val streamRepeatCount = 5

    val streamingGraphBuilder = mock[GenericStreamingGraphBuilder]

    expecting {
      streamingGraphBuilder.produce(EasyMock.anyObject()).andReturn(ZStream.range(0, streamRepeatCount)).times(1)
    }

    replay(streamingGraphBuilder)

    val lifetimeService = TestStreamLifetimeService(streamRepeatCount*2)
    val gb = GenericBackfillStreamingMergeDataProvider(streamingGraphBuilder, lifetimeService, mock[HookManager])

    // Act
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(gb.requestBackfill)).map { result =>
      // Assert
      verify(streamingGraphBuilder)
      result shouldBe a[Unit]
    }
  }

  it should "not produce backfill batch if stream was cancelled" in {
    // Arrange
    val streamRepeatCount = 5

    val streamingGraphBuilder = mock[GenericStreamingGraphBuilder]

    expecting {
      streamingGraphBuilder.produce(EasyMock.anyObject()).andReturn(ZStream.repeat(Chunk.empty)).times(1)
    }

    replay(streamingGraphBuilder)

    val lifetimeService = TestStreamLifetimeService(streamRepeatCount * 2)
    val gb = GenericBackfillStreamingMergeDataProvider(streamingGraphBuilder, lifetimeService, mock[HookManager]) 
    // Act
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(gb.requestBackfill)).map { result =>
      // Assert
      verify(streamingGraphBuilder)
      result shouldBe a[Unit]
    }
  }

  it should "target intermediate table while running" in {
    // Arrange
    val streamRepeatCount = 5

    val testInput = List(
      List(DataCell("name", ArcaneType.StringType, "John Doe"), DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")),
      List(DataCell("name", ArcaneType.StringType, "John"), DataCell("family_name", ArcaneType.StringType, "Doe"), DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")),
    )

    val disposeServiceClient = mock[DisposeServiceClient]
    val mergeServiceClient = mock[MergeServiceClient]
    val jdbcTableManager = mock[JdbcTableManager]
    val hookManager = mock[HookManager]
    val streamDataProvider = mock[StreamDataProvider]

    expecting {

      streamDataProvider.stream.andReturn(ZStream.fromIterable(testInput).repeat(Schedule.forever))

      hookManager
        .onStagingTablesComplete(EasyMock.anyObject(), EasyMock.anyLong(), EasyMock.anyObject())
        .andReturn(new TestIndexedStagedBatches(List.empty, 0))
        .times(streamRepeatCount)

      jdbcTableManager.cleanupStagingTables(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject())
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

      // Validates that the merge service client is called ``streamRepeatCount`` times using the targetTableFullName
      hookManager
        .onBatchStaged(EasyMock.anyObject(), EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject(), EasyMock.eq(TestTargetTableSettings.targetTableFullName), EasyMock.anyObject())
        .andReturn(SqlServerChangeTrackingMergeBatch("test", ArcaneSchema(Seq(MergeKeyField)), "test", TablePropertiesSettings))
        .times(streamRepeatCount)
    }
    replay(streamDataProvider, hookManager, jdbcTableManager)

    val gb = ZIO.service[GenericBackfillStreamingMergeDataProvider].provide(
      // Real services
      GenericStreamingGraphBuilder.layer,
      GenericGroupingTransformer.layer,
      DisposeBatchProcessor.layer,
      FieldFilteringTransformer.layer,
      MergeBatchProcessor.layer,
      StagingProcessor.layer,
      FieldsFilteringService.layer,
      GenericBackfillStreamingMergeDataProvider.layer,
      IcebergS3CatalogWriter.layer,

      // Settings
      ZLayer.succeed(TestGroupingSettings),
      ZLayer.succeed(TestStagingDataSettings),
      ZLayer.succeed(TablePropertiesSettings),
      ZLayer.succeed(TestTargetTableSettings),
      ZLayer.succeed(defaultSettings),
      ZLayer.succeed(TestFieldSelectionRuleSettings),

      // Mocks
      ZLayer.succeed(TestBackfillTableSettings),
      ZLayer.succeed(new BackfillOverwriteBatchFactory {
        override def createBackfillBatch: Task[StagedBackfillOverwriteBatch] =
          ZIO.succeed(SynapseLinkBackfillOverwriteBatch("table", Seq(), "targetName", TestTablePropertiesSettings))
      }),
      ZLayer.succeed(new TestStreamLifetimeService(streamRepeatCount - 1, identity)),
      ZLayer.succeed(disposeServiceClient),
      ZLayer.succeed(mergeServiceClient),
      ZLayer.succeed(jdbcTableManager),
      ZLayer.succeed(hookManager),
      ZLayer.succeed(streamDataProvider),
      ZLayer.succeed(new StreamContext {
        override def IsBackfilling: Boolean = false
      }),
      DeclaredMetrics.layer,
      ArcaneDimensionsProvider.layer,
      ZLayer.succeed(TestSourceBufferingSettings)
    )

    // Act
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(gb.flatMap(_.requestBackfill))).map { result =>
      // Assert
      verify(hookManager)
      result shouldBe a[Unit]
    }
  }
