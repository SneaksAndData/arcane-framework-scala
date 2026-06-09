package com.sneaksanddata.arcane.framework
package tests.services.streaming.graph

import models.batches.{MergeableBatch, StagedVersionedBatch, WatermarkOnlyBatch}
import models.queries.{MergeQuery, OnSegment, WhenMatchedUpdate, WhenNotMatchedInsert}
import models.schemas.*
import models.schemas.ArcaneType.{IntType, StringType}
import services.filters.FieldsFilteringService
import services.iceberg.base.{SinkPropertyManager, StagingEntityManager}
import services.iceberg.{IcebergCatalogFactory, IcebergS3CatalogWriter, IcebergStagingEntityManager}
import services.merging.JdbcMergeServiceClient
import services.merging.cleanup.CatalogDisposeServiceClient
import services.metrics.DeclaredMetrics
import services.naming.DefaultNameGenerator
import services.streaming.base.{StreamDataProvider, StructuredZStream, TimestampOnlyWatermark}
import services.streaming.batching.StagedBatchFactory
import services.streaming.graph.DefaultStreamingGraphBuilder
import services.streaming.processors.batch_processors.maintenance.TargetMaintenanceProcessor
import services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor, SchemaMigrationProcessor, WatermarkProcessor}
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import tests.shared.*
import tests.shared.IcebergCatalogInfo.defaultIcebergStagingSettings

import zio.stream.ZStream
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Ref, Scope, Task, ZIO, ZLayer}

import java.time.OffsetDateTime

final class TestStreamDataProvider(data: Seq[DataRow], schema: ArcaneSchema)
  extends StreamDataProvider:
  override def stream: ZStream[Any, Throwable, StructuredZStream] = ZStream.succeed(
    (ZStream.fromIterable(data), schema)
  )

final class StreamTestBatch(stagedTableName: String, tableName: String, batchSchema: ArcaneSchema)
  extends StagedVersionedBatch
    with MergeableBatch:
  override val name: String                            = stagedTableName
  override val schema: ArcaneSchema                    = batchSchema
  override val targetTableName: String                 = tableName
  override val completedWatermarkValue: Option[String] = None
  override val batchQuery: MergeQuery = MergeQuery(tableName, s"select * from $stagedTableName") ++ OnSegment(
    Map(),
    "ARCANE_MERGE_KEY",
    Seq()
  ) ++ new WhenNotMatchedInsert {
    override val columns: Seq[String]             = Seq("colA", "colB", "ARCANE_MERGE_KEY")
    override val segmentCondition: Option[String] = None
  } ++ new WhenMatchedUpdate {
    override val columns: Seq[String]             = Seq("colA", "colB")
    override val segmentCondition: Option[String] = None
  }
  override def reduceExpr: String = ""

final class StreamWatermarkOnlyTestBatch(tableName: String, watermark: String) extends WatermarkOnlyBatch:
  override val name: String            = "watermark"
  override val schema: ArcaneSchema    = ArcaneSchema.empty()
  override val targetTableName: String = tableName

  override val batchQuery: MergeQuery                  = MergeQuery("test", "test")
  override val completedWatermarkValue: Option[String] = Some(watermark)

  override def reduceExpr: String = ""

final class StreamTestBatchFactory extends StagedBatchFactory:
  override type OutputBatch    = StreamTestBatch
  override type WatermarkBatch = StreamWatermarkOnlyTestBatch

  override def createWatermarkBatch(targetTableName: String, watermark: String): Task[StreamWatermarkOnlyTestBatch] =
    ZIO.succeed(new StreamWatermarkOnlyTestBatch(targetTableName, watermark))

  override def createDataBatch(
                                stagedTableName: String,
                                targetTableName: String,
                                batchSchema: ArcaneSchema
                              ): Task[StreamTestBatch] =
    ZIO.succeed(new StreamTestBatch(stagedTableName, targetTableName, batchSchema))


object DefaultStreamingGraphBuilderTests extends ZIOSpecDefault:
  private val icebergUtil = IcebergUtil(TestDynamicSinkSettings("test").icebergCatalog)
  private val writerLayer: ZLayer[Any, Throwable, IcebergS3CatalogWriter] = ZLayer.scoped {
    for
      factory <- IcebergCatalogFactory.live(defaultIcebergStagingSettings)
      entityManager = IcebergStagingEntityManager(defaultIcebergStagingSettings, factory)
      result = IcebergS3CatalogWriter(entityManager, TestStagingSettings())
    yield result
  }
  private val streamSchema = ArcaneSchema(
    Seq(IndexedMergeKeyField(1), IndexedField("colA", StringType, 2), IndexedField("colB", IntType, 3))
  )
  private val mergeServiceClient = new JdbcMergeServiceClient(
    TestJdbcMergeServiceClientSettings,
    "iceberg",
    "test",
    DeclaredMetrics(),
    false
  )

  private def getStreamBuilder(input: Seq[DataRow], targetName: String) = for
    writer <- ZIO.service[IcebergS3CatalogWriter]
    sinkPropertyManager <- ZIO.service[SinkPropertyManager]
    stagingEntityManager <- ZIO.service[StagingEntityManager]
    schemaMigrationProcessor <- ZIO.service[SchemaMigrationProcessor]
    counter     <- Ref.make(0L)
    nameGenerator <- ZIO.succeed(
      new DefaultNameGenerator(
        sinkSettings = TestDynamicSinkSettings(targetName),
        backfillId = "",
        streamId = "default-streaming-graph-builder-tests"
      )
    )
    // target table should exist
    _ <- icebergUtil.prepareWatermark(
      targetName,
      TimestampOnlyWatermark(OffsetDateTime.now()),
      Some(streamSchema)
    )
    builder <- ZIO.succeed(
      DefaultStreamingGraphBuilder(
        streamDataProvider = new TestStreamDataProvider(input, streamSchema),
        fieldFilteringProcessor = new FieldFilteringTransformer(new FieldsFilteringService(TestFieldSelectionRuleSettings)),
        stagingProcessor = new StagingProcessor(
          targetTableFullName = s"iceberg.test.$targetName",
          icebergCatalogSettings = defaultIcebergStagingSettings,
          catalogWriter = writer,
          batchFactory = new StreamTestBatchFactory(),
          declaredMetrics = DeclaredMetrics(),
          nameGenerator = nameGenerator
        ),
        mergeProcessor = new MergeBatchProcessor(
          mergeServiceClient = mergeServiceClient,
          declaredMetrics = DeclaredMetrics()
        ),
        disposeBatchProcessor = new DisposeBatchProcessor(
          new CatalogDisposeServiceClient(
            stagingEntityManager
          ),
          false
        ),
        watermarkProcessor = new WatermarkProcessor(
          propertyManager = sinkPropertyManager,
          targetTableShortName = targetName,
          declaredMetrics = DeclaredMetrics()
        ),
        schemaMigrationProcessor = schemaMigrationProcessor,
        targetMaintenanceProcessor = new TargetMaintenanceProcessor(
          counterRef = counter,
          options = TestJdbcMergeServiceClientSettings,
          maintenanceSettings = TestTableMaintenanceSettings,
          defaultCatalogName = "iceberg",
          defaultSchemaName = "test",
          declaredMetrics = DeclaredMetrics(),
          isBackfilling = false
        )
      )
    )
  yield builder

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("DefaultStreamingGraphBuilderTests")(
    test("produces a functional stream graph that runs indefinitely") {
      for
        rowsToStream <- ZIO.succeed(
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
            ),
            JsonWatermarkRow(TimestampOnlyWatermark(OffsetDateTime.now()))
          )
        )
        builder <- getStreamBuilder(rowsToStream, "stream_builder")
        result <- builder.produce().runCollect
      yield assertTrue(result.size == 2)
    }
  ).provide(
    writerLayer,
    VoidSchemaMigrationProcessor.layer,
    icebergUtil.getSinkEntityManagerLayer,
    icebergUtil.getStagingEntityManagerLayer,
    icebergUtil.getSinkTablePropertyManagerLayer
  ) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock
