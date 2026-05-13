package com.sneaksanddata.arcane.framework
package tests.services.streaming.data_providers.backfill

import models.*
import models.batches.{StagedBackfillOverwriteBatch, SynapseLinkBackfillOverwriteBatch}
import models.schemas.*
import models.schemas.ArcaneType.StringType
import services.base.{BatchDisposeResult, DisposeServiceClient, MergeServiceClient}
import services.filters.FieldsFilteringService
import services.iceberg.{IcebergEntityManager, IcebergS3CatalogWriter, IcebergTablePropertyManager}
import services.metrics.base.MetricTagProvider
import services.metrics.{DeclaredMetrics, GlobalMetricTagProvider}
import services.streaming.base.StreamDataProvider
import services.streaming.processors.batch_processors.maintenance.TargetMaintenanceProcessor
import services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  SchemaMigrationProcessor,
  WatermarkProcessor
}
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import tests.services.streaming.processors.utils.TestStageVersionedBatch
import tests.shared.*
import com.sneaksanddata.arcane.framework.services.backfill.{BackfillOverwriteBatchFactory, BackfillStreamingOverwriteDataProvider, DefaultBackfillStreamDataProvider}
import com.sneaksanddata.arcane.framework.services.streaming.graph.DefaultStreamingGraphBuilder

import org.easymock.{Capture, EasyMock}
import org.easymock.EasyMock.{replay, verify}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.shouldBe
import org.scalatestplus.easymock.EasyMockSugar
import zio.stream.ZStream
import zio.{Runtime, Schedule, Task, Unsafe, ZIO, ZLayer}

class GenericBackfillStreamingOverwriteDataProviderTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
  private val runtime = Runtime.default

  it should "produce backfill batch if stream is completed" in {
    // Arrange
    val streamRepeatCount = 5

    val streamingGraphBuilder = mock[DefaultStreamingGraphBuilder]
    expecting {
      streamingGraphBuilder
        .produce()
        .andReturn(
          ZStream.fromIterable(
            Seq(
              TestStageVersionedBatch(
                "test",
                ArcaneSchema(Seq(Field(name = "test", fieldType = StringType))),
                "target_test",
                TestTablePropertiesSettings,
                "col0",
                Some("123")
              )
            )
          )
        )
        .times(1)
    }

    replay(streamingGraphBuilder)

    val lifetimeService = TestStreamLifetimeService(streamRepeatCount * 2)
    val gb = DefaultBackfillStreamDataProvider(
      streamingGraphBuilder,
      TestStagingTableSettings,
      lifetimeService,
      (watermark: Option[String]) =>
        ZIO.succeed(
          SynapseLinkBackfillOverwriteBatch("table", Seq(), "targetName", TestTablePropertiesSettings, watermark)
        ),
      mock[MetricTagProvider]
    )

    // Act
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(gb.requestBackfill)).map { result =>
      // Assert
      verify(streamingGraphBuilder)
      result shouldBe a[SynapseLinkBackfillOverwriteBatch]
    }
  }

  it should "not produce backfill batch if stream was cancelled" in {
    // Arrange
    val streamRepeatCount = 5

    val streamingGraphBuilder = mock[DefaultStreamingGraphBuilder]

    expecting {
      streamingGraphBuilder
        .produce()
        .andReturn(
          ZStream.repeat(
            TestStageVersionedBatch(
              "test",
              ArcaneSchema(Seq(Field(name = "test", fieldType = StringType))),
              "target_test",
              TestTablePropertiesSettings,
              "col0",
              Some("123")
            )
          )
        )
        .times(1)
    }

    replay(streamingGraphBuilder)

    val lifetimeService = TestStreamLifetimeService(streamRepeatCount * 2)
    val gb = DefaultBackfillStreamDataProvider(
      streamingGraphBuilder,
      TestStagingTableSettings,
      lifetimeService,
      (watermark: Option[String]) =>
        ZIO.succeed(
          SynapseLinkBackfillOverwriteBatch("table", Seq(), "targetName", TestTablePropertiesSettings, watermark)
        ),
      mock[MetricTagProvider]
    )

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
      List(
        DataCell("name", ArcaneType.StringType, "John Doe"),
        DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")
      ),
      List(
        DataCell("name", ArcaneType.StringType, "John"),
        DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")
      )
    )

    val disposeServiceClient = mock[DisposeServiceClient]
    val mergeServiceClient   = mock[MergeServiceClient]
    val streamDataProvider   = mock[StreamDataProvider]

    val batchCapture = Capture.newInstance[TestMergeBatch]

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

      mergeServiceClient.applyBatch(EasyMock.capture(batchCapture)).andReturn(ZIO.succeed(true)).times(5)
      disposeServiceClient.disposeBatch(EasyMock.anyObject()).andReturn(ZIO.succeed(BatchDisposeResult(true))).times(5)

    }
    replay(streamDataProvider, mergeServiceClient, disposeServiceClient)

    val gb = ZLayer.make[BackfillStreamingOverwriteDataProvider](
      // Real services
      DefaultStreamingGraphBuilder.layer,
      DisposeBatchProcessor.layer,
      FieldFilteringTransformer.layer,
      MergeBatchProcessor.layer,
      StagingProcessor.layer,
      FieldsFilteringService.layer,
      DefaultBackfillStreamDataProvider.layer,
      IcebergEntityManager.stagingLayer,
      IcebergEntityManager.sinkLayer,
      IcebergS3CatalogWriter.layer,

      // Mocks
      ZLayer.succeed(new BackfillOverwriteBatchFactory {
        override def createBackfillBatch(watermark: Option[String]): Task[StagedBackfillOverwriteBatch] =
          ZIO.succeed(
            SynapseLinkBackfillOverwriteBatch("table", Seq(), "targetName", TestTablePropertiesSettings, watermark)
          )
      }),
      ZLayer.succeed(new TestStreamLifetimeService(streamRepeatCount, identity)),
      ZLayer.succeed(disposeServiceClient),
      ZLayer.succeed(mergeServiceClient),
      ZLayer.succeed(streamDataProvider),
      ZLayer.succeed(TestPluginStreamContext),
      ZLayer.succeed(new TestStagedBatchFactory()),
      TargetMaintenanceProcessor.layer,
      VoidSchemaMigrationProcessor.layer,
      DeclaredMetrics.layer,
      GlobalMetricTagProvider.layer,
      WatermarkProcessor.layer,
      IcebergTablePropertyManager.sinkLayer,
      IcebergTablePropertyManager.stagingLayer
    )

    // Act
    Unsafe
      .unsafe(implicit unsafe =>
        runtime.unsafe
          .runToFuture(ZIO.service[BackfillStreamingOverwriteDataProvider].flatMap(_.requestBackfill).provideLayer(gb))
      )
      .map { result =>
        // Assert
        verify(streamDataProvider, mergeServiceClient, disposeServiceClient)
        result shouldBe a[Unit]
        batchCapture.getValue.name.startsWith("staging_table__") shouldBe true
      }
  }
