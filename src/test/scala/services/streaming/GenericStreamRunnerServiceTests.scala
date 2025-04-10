package com.sneaksanddata.arcane.framework
package services.streaming

import models.*
import services.app.GenericStreamRunnerService
import services.app.base.StreamRunnerService
import services.base.{BatchOptimizationResult, DisposeServiceClient, MergeServiceClient}
import services.consumers.SqlServerChangeTrackingMergeBatch
import services.filters.FieldsFilteringService
import services.lakehouse.base.{CatalogWriter, IcebergCatalogSettings, S3CatalogFileIO}
import services.merging.JdbcTableManager
import services.streaming.base.{BackfillStreamingDataProvider, HookManager, StreamDataProvider}
import services.streaming.processors.GenericGroupingTransformer
import services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor}
import services.streaming.processors.transformers.FieldFilteringTransformer.Environment
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import services.streaming.processors.utils.TestIndexedStagedBatches
import utils.*

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.services.lakehouse.{IcebergCatalogCredential, IcebergS3CatalogWriter}
import com.sneaksanddata.arcane.framework.services.mssql.MssqlBackfillDataProvider
import com.sneaksanddata.arcane.framework.services.streaming.graph_builders.GenericStreamingGraphBuilder
import com.sneaksanddata.arcane.framework.services.streaming.graph_builders.backfill.GenericBackfillOverwriteGraphBuilder
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import org.easymock.EasyMock
import org.easymock.EasyMock.{replay, verify}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import org.scalatestplus.easymock.EasyMockSugar
import zio.stream.ZStream
import zio.{Chunk, Runtime, Schedule, Unsafe, ZIO, ZLayer}

class GenericStreamRunnerServiceTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
  private val runtime = Runtime.default
  private val settings = new IcebergCatalogSettings:
    override val namespace = "test"
    override val warehouse = "demo"
    override val catalogUri = "http://localhost:20001/catalog"
    override val additionalProperties: Map[String, String] = IcebergCatalogCredential.oAuth2Properties
    override val s3CatalogFileIO: S3CatalogFileIO = S3CatalogFileIO
    override val stagingLocation: Option[String] = None  

  private val testInput = List(
    List(DataCell("name", ArcaneType.StringType, "John Doe"), DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")),
    List(DataCell("name", ArcaneType.StringType, "John"), DataCell("family_name", ArcaneType.StringType, "Doe"), DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")),
  )

  it should "gracefully handle stream shutdown" in {
    // Arrange
    val streamRepeatCount = 5

    val disposeServiceClient = mock[DisposeServiceClient]
    val mergeServiceClient = mock[MergeServiceClient]
    val jdbcTableManager = mock[JdbcTableManager]
    val hookManager = mock[HookManager]
    val streamDataProvider = mock[StreamDataProvider]

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
        .onBatchStaged(EasyMock.anyObject(), EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject(), EasyMock.anyString(), EasyMock.anyObject())
        .andReturn(SqlServerChangeTrackingMergeBatch("test", ArcaneSchema(Seq(MergeKeyField)), "test", TablePropertiesSettings))
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
    }
    replay(streamDataProvider, hookManager, jdbcTableManager)

    val streamRunnerService = ZIO.service[StreamRunnerService].provide(
      // Real services
      GenericStreamRunnerService.layer,
      GenericStreamingGraphBuilder.layer,
      GenericGroupingTransformer.layer,
      DisposeBatchProcessor.layer,
      FieldFilteringTransformer.layer,
      MergeBatchProcessor.layer,
      StagingProcessor.layer,
      FieldsFilteringService.layer,
      IcebergS3CatalogWriter.layer,

      // Settings
      ZLayer.succeed(TestGroupingSettings),
      ZLayer.succeed(TestStagingDataSettings),
      ZLayer.succeed(TablePropertiesSettings),
      ZLayer.succeed(TestTargetTableSettings),
      ZLayer.succeed(settings),
      ZLayer.succeed(TestFieldSelectionRuleSettings),

      // Mocks
      ZLayer.succeed(new TestStreamLifetimeService(streamRepeatCount-1, identity)),
      ZLayer.succeed(disposeServiceClient),
      ZLayer.succeed(mergeServiceClient),
      ZLayer.succeed(jdbcTableManager),
      ZLayer.succeed(hookManager),
      ZLayer.succeed(streamDataProvider),
      ZLayer.succeed(new StreamContext {
        override def IsBackfilling: Boolean = false
      })
    )

    // Act
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(streamRunnerService.flatMap(_.run))).map { _ =>
      // Assert
      noException should be thrownBy verify(streamDataProvider, hookManager)
    }
  }
