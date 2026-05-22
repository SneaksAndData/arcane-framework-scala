package com.sneaksanddata.arcane.framework
package tests.services.backfill

import models.queries.StreamingBatchQuery
import models.schemas.ArcaneType.{IntType, StringType}
import models.schemas.*
import models.settings.TableNaming.getBackfillTableName
import models.sharding.*
import services.backfill.DefaultBackfillStateManager
import services.backfill.base.{BackfillStreamDataProvider, ShardFactory}
import services.backfill.graph.DefaultBackfillOverwriteGraphBuilder
import services.backfill.processors.{BackfillCompletionProcessor, ShardStagingProcessor}
import services.filters.FieldsFilteringService
import services.iceberg.base.{SinkPropertyManager, StagingEntityManager, StagingPropertyManager}
import services.iceberg.{IcebergCatalogFactory, IcebergS3CatalogWriter, IcebergStagingEntityManager}
import services.merging.JdbcMergeServiceClient
import services.metrics.DeclaredMetrics
import services.streaming.base.{JsonWatermark, TimestampOnlyWatermark}
import services.streaming.processors.transformers.FieldFilteringTransformer
import services.synapse.backfill.SynapseShardFactory
import tests.shared.*
import tests.shared.IcebergCatalogInfo.defaultIcebergStagingSettings

import zio.stream.ZStream
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, Task, ZIO, ZLayer}

import java.time.OffsetDateTime

final class TestBackfillStreamDataProvider(schema: ArcaneSchema) extends BackfillStreamDataProvider:
  override def backfillStream: Task[(stream: ZStream[Any, Throwable, BootstrappedShard], watermark: JsonWatermark)] =
    ZIO.succeed(
      (
        stream = ZStream.fromIterable(
          Seq(
            DefaultBootstrappedShard(
              shardStream = (
                ZStream.fromIterable(
                  Seq(
                    List(
                      DataCell(MergeKeyField.name, StringType, "k1"),
                      DataCell("colA", StringType, "one"),
                      DataCell("colB", IntType, 1)
                    ),
                    List(
                      DataCell(MergeKeyField.name, StringType, "k2"),
                      DataCell("colA", StringType, "two"),
                      DataCell("colB", IntType, 2)
                    )
                  )
                ),
                schema
              ),
              "shard1",
              "backfill__generic__test_combined",
              "iceberg.test.generic_stream",
              "test_combined"
            ),
            DefaultBootstrappedShard(
              shardStream = (
                ZStream.fromIterable(
                  Seq(
                    List(
                      DataCell(MergeKeyField.name, StringType, "k3"),
                      DataCell("colA", StringType, "three"),
                      DataCell("colB", IntType, 3)
                    ),
                    List(
                      DataCell(MergeKeyField.name, StringType, "k4"),
                      DataCell("colA", StringType, "four"),
                      DataCell("colB", IntType, 4)
                    )
                  )
                ),
                schema
              ),
              "shard2",
              "backfill__generic__test_combined",
              "iceberg.test.generic_stream",
              "test_combined"
            ),
            DefaultBootstrappedShard(
              shardStream = (
                ZStream.fromIterable(
                  Seq(
                    List(
                      DataCell(MergeKeyField.name, StringType, "k5"),
                      DataCell("colA", StringType, "five"),
                      DataCell("colB", IntType, 5)
                    )
                  )
                ),
                schema
              ),
              "shard3",
              "backfill__generic__test_combined",
              "iceberg.test.generic_stream",
              "test_combined"
            )
          )
        ),
        watermark = TimestampOnlyWatermark(OffsetDateTime.now())
      )
    )

final class TestShardFactory extends ShardFactory:
  override def createStagedShard(shard: BootstrappedShard): StagedShard = DefaultStagedShard(
    shard.shardSourceEntityName,
    shard.combinedTableName,
    shard.targetTableName,
    new StreamingBatchQuery {
      override def query: String = s"INSERT INTO ${shard.combinedTableName} SELECT * FROM ${shard.shardTableName}"
    },
    shard.backfillId
  )

  override def createCompletionShard(shard: StagedShard, watermark: String): CompletionShard = CompletionShard(
    watermark = watermark,
    targetTableName = shard.targetTableName,
    shardSourceEntityName = shard.shardSourceEntityName,
    combinedTableName = shard.combinedTableName,
    commitQuery = new StreamingBatchQuery {
      override def query: String =
        s"CREATE OR REPLACE TABLE ${shard.targetTableName} AS SELECT * FROM ${shard.combinedTableName}"
    },
    shard.backfillId
  )

object DefaultBackfillOverwriteGraphBuilderTests extends ZIOSpecDefault:
  private val streamSchema = ArcaneSchema(
    Seq(IndexedMergeKeyField(1), IndexedField("colA", StringType, 2), IndexedField("colB", IntType, 3))
  )
  private val writerLayer: ZLayer[Any, Throwable, IcebergS3CatalogWriter] = ZLayer.scoped {
    for
      factory <- IcebergCatalogFactory.live(defaultIcebergStagingSettings)
      entityManager = IcebergStagingEntityManager(defaultIcebergStagingSettings, factory)
      result        = IcebergS3CatalogWriter(entityManager, TestStagingSettings())
    yield result
  }
  private val icebergUtilBackfill = IcebergUtil(TestDynamicSinkSettings("test").icebergCatalog)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("DefaultBackfillOverwriteGraphBuilderTests")(
    test("stages shards, aggregates them and swaps correct data into target table") {
      for
        writer <- ZIO.service[IcebergS3CatalogWriter]
        mergeService <- ZIO.succeed(
          new JdbcMergeServiceClient(
            TestJdbcMergeServiceClientSettings,
            "iceberg",
            "test",
            DeclaredMetrics(),
            true
          )
        )
        // shaper requires target table to exist
        _ <- icebergUtilBackfill.prepareBackfillTable(getBackfillTableName("generic__test_combined"), streamSchema)
        shardFactory           <- ZIO.succeed(new TestShardFactory())
        propertyManager        <- ZIO.service[SinkPropertyManager]
        stagingPropertyManager <- ZIO.service[StagingPropertyManager]
        stagingEntityManager   <- ZIO.service[StagingEntityManager]
        backfillStateManager <- ZIO.succeed(
          new DefaultBackfillStateManager(
            stagingEntityManager,
            stagingPropertyManager,
            new SynapseShardFactory(),
            getBackfillTableName("synapse__backfill_new")
          )
        )
        builder <- ZIO.succeed(
          DefaultBackfillOverwriteGraphBuilder(
            new TestBackfillStreamDataProvider(streamSchema),
            new ShardStagingProcessor(
              writer,
              shardFactory,
              DeclaredMetrics()
            ),
            mergeService,
            new FieldFilteringTransformer(new FieldsFilteringService(TestFieldSelectionRuleSettings)),
            new BackfillCompletionProcessor(propertyManager, mergeService, DeclaredMetrics()),
            backfillStateManager,
            shardFactory
          )
        )
        // expect the following:
        // a single row - CompletedShard is the output
        result <- builder.produce().runCollect
      yield assertTrue(result.size == 1)
    }
  ).provide(
    writerLayer,
    icebergUtilBackfill.getStagingTablePropertyManagerLayer,
    icebergUtilBackfill.getStagingEntityManagerLayer,
    icebergUtilBackfill.getSinkTablePropertyManagerLayer
  ) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock

//
//class GenericBackfillStreamingOverwriteDataProviderTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
//  private val runtime = Runtime.default
//
//  it should "produce backfill batch if stream is completed" in {
//    // Arrange
//    val streamRepeatCount = 5
//
//    val streamingGraphBuilder = mock[DefaultStreamingGraphBuilder]
//    expecting {
//      streamingGraphBuilder
//        .produce()
//        .andReturn(
//          ZStream.fromIterable(
//            Seq(
//              TestStageVersionedBatch(
//                "test",
//                ArcaneSchema(Seq(Field(name = "test", fieldType = StringType))),
//                "target_test",
//                TestTablePropertiesSettings,
//                "col0",
//                Some("123")
//              )
//            )
//          )
//        )
//        .times(1)
//    }
//
//    replay(streamingGraphBuilder)
//
//    val lifetimeService = TestStreamLifetimeService(streamRepeatCount * 2)
//    val gb = DefaultBackfillStreamDataProvider(
//      streamingGraphBuilder,
//      TestStagingTableSettings,
//      lifetimeService,
//      (watermark: Option[String]) =>
//        ZIO.succeed(
//          SynapseLinkBackfillOverwriteBatch("table", Seq(), "targetName", TestTablePropertiesSettings, watermark)
//        ),
//      mock[MetricTagProvider]
//    )
//
//    // Act
//    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(gb.requestBackfill)).map { result =>
//      // Assert
//      verify(streamingGraphBuilder)
//      result shouldBe a[SynapseLinkBackfillOverwriteBatch]
//    }
//  }
//
//  it should "not produce backfill batch if stream was cancelled" in {
//    // Arrange
//    val streamRepeatCount = 5
//
//    val streamingGraphBuilder = mock[DefaultStreamingGraphBuilder]
//
//    expecting {
//      streamingGraphBuilder
//        .produce()
//        .andReturn(
//          ZStream.repeat(
//            TestStageVersionedBatch(
//              "test",
//              ArcaneSchema(Seq(Field(name = "test", fieldType = StringType))),
//              "target_test",
//              TestTablePropertiesSettings,
//              "col0",
//              Some("123")
//            )
//          )
//        )
//        .times(1)
//    }
//
//    replay(streamingGraphBuilder)
//
//    val lifetimeService = TestStreamLifetimeService(streamRepeatCount * 2)
//    val gb = DefaultBackfillStreamDataProvider(
//      streamingGraphBuilder,
//      TestStagingTableSettings,
//      lifetimeService,
//      (watermark: Option[String]) =>
//        ZIO.succeed(
//          SynapseLinkBackfillOverwriteBatch("table", Seq(), "targetName", TestTablePropertiesSettings, watermark)
//        ),
//      mock[MetricTagProvider]
//    )
//
//    // Act
//    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(gb.requestBackfill)).map { result =>
//      // Assert
//      verify(streamingGraphBuilder)
//      result shouldBe a[Unit]
//    }
//  }
//
//  it should "target intermediate table while running" in {
//    // Arrange
//    val streamRepeatCount = 5
//
//    val testInput = List(
//      List(
//        DataCell("name", ArcaneType.StringType, "John Doe"),
//        DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")
//      ),
//      List(
//        DataCell("name", ArcaneType.StringType, "John"),
//        DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")
//      )
//    )
//
//    val disposeServiceClient = mock[DisposeServiceClient]
//    val mergeServiceClient   = mock[MergeServiceClient]
//    val streamDataProvider   = mock[StreamDataProvider]
//
//    val batchCapture = Capture.newInstance[TestMergeBatch]
//
//    expecting {
//
//      // The data provider mock provides an infinite stream of test input
//      streamDataProvider.stream.andReturn(
//        ZStream.succeed(
//          (
//            ZStream.fromIterable(testInput).repeat(Schedule.forever).rechunk(1),
//            ArcaneSchema(Seq(IndexedMergeKeyField(1), IndexedField("name", StringType, 2)))
//          )
//        )
//      )
//
//      mergeServiceClient.applyBatch(EasyMock.capture(batchCapture)).andReturn(ZIO.succeed(true)).times(5)
//      disposeServiceClient.disposeBatch(EasyMock.anyObject()).andReturn(ZIO.succeed(BatchDisposeResult(true))).times(5)
//
//    }
//    replay(streamDataProvider, mergeServiceClient, disposeServiceClient)
//
//    val gb = ZLayer.make[BackfillStreamingOverwriteDataProvider](
//      // Real services
//      DefaultStreamingGraphBuilder.layer,
//      DisposeBatchProcessor.layer,
//      FieldFilteringTransformer.layer,
//      MergeBatchProcessor.layer,
//      StagingProcessor.layer,
//      FieldsFilteringService.layer,
//      DefaultBackfillStreamDataProvider.layer,
//      IcebergEntityManager.stagingLayer,
//      IcebergEntityManager.sinkLayer,
//      IcebergS3CatalogWriter.layer,
//
//      // Mocks
//      ZLayer.succeed(new BackfillOverwriteBatchFactory {
//        override def createBackfillBatch(watermark: Option[String]): Task[StagedBackfillOverwriteBatch] =
//          ZIO.succeed(
//            SynapseLinkBackfillOverwriteBatch("table", Seq(), "targetName", TestTablePropertiesSettings, watermark)
//          )
//      }),
//      ZLayer.succeed(new TestStreamLifetimeService(streamRepeatCount, identity)),
//      ZLayer.succeed(disposeServiceClient),
//      ZLayer.succeed(mergeServiceClient),
//      ZLayer.succeed(streamDataProvider),
//      ZLayer.succeed(TestPluginStreamContext),
//      ZLayer.succeed(new TestStagedBatchFactory()),
//      TargetMaintenanceProcessor.layer,
//      VoidSchemaMigrationProcessor.layer,
//      DeclaredMetrics.layer,
//      GlobalMetricTagProvider.layer,
//      WatermarkProcessor.layer,
//      IcebergTablePropertyManager.sinkLayer,
//      IcebergTablePropertyManager.stagingLayer
//    )
//
//    // Act
//    Unsafe
//      .unsafe(implicit unsafe =>
//        runtime.unsafe
//          .runToFuture(ZIO.service[BackfillStreamingOverwriteDataProvider].flatMap(_.requestBackfill).provideLayer(gb))
//      )
//      .map { result =>
//        // Assert
//        verify(streamDataProvider, mergeServiceClient, disposeServiceClient)
//        result shouldBe a[Unit]
//        batchCapture.getValue.name.startsWith("staging_table__") shouldBe true
//      }
//  }
