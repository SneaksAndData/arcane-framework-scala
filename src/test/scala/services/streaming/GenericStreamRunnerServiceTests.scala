package com.sneaksanddata.arcane.framework
package services.streaming

import models.{ArcaneSchema, ArcaneType, DataCell, DataRow, MergeKeyField}
import services.app.GenericStreamRunnerService
import services.app.base.StreamRunnerService
import services.base.{DisposeServiceClient, MergeServiceClient}
import services.filters.FieldsFilteringService
import services.lakehouse.base.CatalogWriter
import services.merging.JdbcTableManager
import services.streaming.base.{HookManager, StreamDataProvider}
import services.streaming.graph_builders.base.GenericStreamingGraphBuilder
import services.streaming.processors.GenericGroupingTransformer
import services.streaming.processors.batch_processors.{DisposeBatchProcessor, MergeBatchProcessor}
import services.streaming.processors.transformers.FieldFilteringTransformer.Environment
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import services.streaming.processors.utils.TestIndexedStagedBatches
import utils.*

import com.sneaksanddata.arcane.framework.services.consumers.SqlServerChangeTrackingMergeBatch
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

    val catalogWriter = mock[CatalogWriter[RESTCatalog, Table, Schema]]
    val tableMock = mock[Table]

    expecting {

      // The table mock, does not verify anything
      tableMock
        .name()
        .andReturn("database.namespace.name")
        .anyTimes()

      // The data provider mock provides an infinite stream of test input
      streamDataProvider.stream.andReturn(ZStream.fromIterable(testInput).repeat(Schedule.forever))

      // The catalogWriter.write method is called ``streamRepeatCount`` times
      catalogWriter
        .write(EasyMock.anyObject[Chunk[DataRow]], EasyMock.anyString(), EasyMock.anyObject())
        .andReturn(ZIO.succeed(tableMock))
        .times(streamRepeatCount)

      // The hookManager.onStagingTablesComplete method is called ``streamRepeatCount`` times
      // It produces the empty set of staged batches, so the rest  of the pipeline can continue
      // but no further stages being invoked
      hookManager
        .onStagingTablesComplete(EasyMock.anyObject(), EasyMock.anyLong(), EasyMock.anyObject())
        .andReturn(new TestIndexedStagedBatches(List.empty, 0))
        .times(streamRepeatCount)
      hookManager
        .onBatchStaged(EasyMock.anyObject(), EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject(), EasyMock.anyString(), EasyMock.anyObject())
        .andReturn(SqlServerChangeTrackingMergeBatch("test", "batchId", ArcaneSchema(Seq(MergeKeyField)), "test", TablePropertiesSettings))
        .times(streamRepeatCount)

      jdbcTableManager.createStagingTable
        .andReturn(ZIO.unit)
        .anyTimes()
      jdbcTableManager.createTargetTable
        .andReturn(ZIO.unit)
        .anyTimes()
      jdbcTableManager.createBackFillTable
        .andReturn(ZIO.unit)
        .anyTimes()
    }
    replay(catalogWriter, streamDataProvider, tableMock, hookManager, jdbcTableManager)

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

      // Settings
      ZLayer.succeed(TestGroupingSettings),
      ZLayer.succeed(TestStagingDataSettings),
      ZLayer.succeed(TablePropertiesSettings),
      ZLayer.succeed(TestTargetTableSettings),
      ZLayer.succeed(TestIcebergCatalogSettings),
      ZLayer.succeed(TestFieldSelectionRuleSettings),

      // Mocks
      ZLayer.succeed(catalogWriter),
      ZLayer.succeed(new TestStreamLifetimeService(streamRepeatCount-1, identity)),
      ZLayer.succeed(disposeServiceClient),
      ZLayer.succeed(mergeServiceClient),
      ZLayer.succeed(jdbcTableManager),
      ZLayer.succeed(hookManager),
      ZLayer.succeed(streamDataProvider)
    )

    // Act
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(streamRunnerService.flatMap(_.run))).map { result =>
      // Assert
      noException should be thrownBy verify(catalogWriter, streamDataProvider, tableMock, hookManager)
    }
  }
