package com.sneaksanddata.arcane.framework
package tests.services.backfill

import models.batches.{MergeableBatch, StagedVersionedBatch, WatermarkOnlyBatch}
import models.queries.{MergeQuery, OnSegment, WhenNotMatchedInsert}
import models.schemas.ArcaneType.{IntType, StringType}
import models.schemas.*
import services.backfill.base.BackfillStreamDataProvider
import services.backfill.graph.DefaultBackfillMergeGraphBuilder
import services.filters.FieldsFilteringService
import services.iceberg.base.{SinkPropertyManager, StagingEntityManager, StagingPropertyManager}
import services.iceberg.{IcebergCatalogFactory, IcebergS3CatalogWriter, IcebergStagingEntityManager}
import services.merging.JdbcMergeServiceClient
import services.metrics.DeclaredMetrics
import services.naming.DefaultNameGenerator
import services.streaming.base.{StructuredZStream, TimestampOnlyWatermark}
import services.streaming.batching.StagedBatchFactory
import services.streaming.processors.batch_processors.streaming.{
  MergeBatchProcessor,
  SchemaMigrationProcessor,
  WatermarkProcessor
}
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import tests.shared.*
import tests.shared.IcebergCatalogInfo.defaultIcebergStagingSettings

import zio.stream.ZStream
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Scope, Task, ZIO, ZLayer}

import java.time.OffsetDateTime

final class TestBackfillMergeStreamDataProvider(data: Seq[DataRow], schema: ArcaneSchema)
    extends BackfillStreamDataProvider:
  override def stream: ZStream[Any, Throwable, StructuredZStream] = ZStream.succeed(
    (ZStream.fromIterable(data), schema)
  )

final class BackfillMergeTestBatch(stagedTableName: String, tableName: String, batchSchema: ArcaneSchema)
    extends StagedVersionedBatch
    with MergeableBatch:
  override val name: String                            = stagedTableName
  override val schema: ArcaneSchema                    = batchSchema
  override val targetTableName: String                 = tableName
  override val completedWatermarkValue: Option[String] = None
  override val batchQuery: MergeQuery = MergeQuery(tableName, s"select * from $stagedTableName") ++ OnSegment(
    Map(),
    "colA",
    Seq()
  ) ++ new WhenNotMatchedInsert {
    override val columns: Seq[String]             = Seq("colA", "colB", "ARCANE_MERGE_KEY")
    override val segmentCondition: Option[String] = None
  }
  override def reduceExpr: String = ""

final class BackfillWatermarkTestOnlyBatch(tableName: String, watermark: String) extends WatermarkOnlyBatch:
  override val name: String            = "watermark"
  override val schema: ArcaneSchema    = ArcaneSchema.empty()
  override val targetTableName: String = tableName

  override val batchQuery: MergeQuery                  = MergeQuery("test", "test")
  override val completedWatermarkValue: Option[String] = Some(watermark)

  override def reduceExpr: String = ""

final class BackfillMergeTestBatchFactory extends StagedBatchFactory:
  override type OutputBatch    = BackfillMergeTestBatch
  override type WatermarkBatch = BackfillWatermarkTestOnlyBatch

  override def createWatermarkBatch(targetTableName: String, watermark: String): Task[BackfillWatermarkTestOnlyBatch] =
    ZIO.succeed(new BackfillWatermarkTestOnlyBatch(targetTableName, watermark))

  override def createDataBatch(
      stagedTableName: String,
      targetTableName: String,
      batchSchema: ArcaneSchema
  ): Task[BackfillMergeTestBatch] =
    ZIO.succeed(new BackfillMergeTestBatch(stagedTableName, targetTableName, batchSchema))

object DefaultBackfillMergeGraphBuilderTests extends ZIOSpecDefault:
  private val icebergUtilBackfill = IcebergUtil(TestDynamicSinkSettings("test").icebergCatalog)
  private val writerLayer: ZLayer[Any, Throwable, IcebergS3CatalogWriter] = ZLayer.scoped {
    for
      factory <- IcebergCatalogFactory.live(defaultIcebergStagingSettings)
      entityManager = IcebergStagingEntityManager(defaultIcebergStagingSettings, factory)
      result        = IcebergS3CatalogWriter(entityManager, TestStagingSettings())
    yield result
  }
  private val streamSchema = ArcaneSchema(
    Seq(IndexedMergeKeyField(1), IndexedField("colA", StringType, 2), IndexedField("colB", IntType, 3))
  )

  private def runBackfill(targetName: String, backfillId: String, changeSet: Seq[DataRow], schema: ArcaneSchema) = for
    writer                   <- ZIO.service[IcebergS3CatalogWriter]
    sinkPropertyManager      <- ZIO.service[SinkPropertyManager]
    stagingEntityManager     <- ZIO.service[StagingEntityManager]
    stagingPropertyManager   <- ZIO.service[StagingPropertyManager]
    schemaMigrationProcessor <- ZIO.service[SchemaMigrationProcessor]
    nameGenerator <- ZIO.succeed(
      new DefaultNameGenerator(
        sinkSettings = TestDynamicSinkSettings(targetName),
        backfillId = backfillId,
        streamId = "default-backfill-merge-graph-builder"
      )
    )
    backfillTableName <- nameGenerator.getBackfillTableName
    // backfill table should exist
    _ <- icebergUtilBackfill.prepareBackfillTable(
      backfillTableName,
      streamSchema,
      recreate = false
    )
    // target table should exist
    _ <- icebergUtilBackfill.prepareWatermark(
      targetName,
      TimestampOnlyWatermark(OffsetDateTime.now()),
      Some(streamSchema)
    )
    mergeService <- ZIO.succeed(
      new JdbcMergeServiceClient(
        TestJdbcMergeServiceClientSettings,
        "iceberg",
        "test",
        DeclaredMetrics(),
        true
      )
    )
    builder <- ZIO.succeed(
      DefaultBackfillMergeGraphBuilder(
        streamDataProvider = new TestBackfillMergeStreamDataProvider(changeSet, schema),
        fieldFilteringProcessor =
          new FieldFilteringTransformer(new FieldsFilteringService(TestFieldSelectionRuleSettings)),
        stagingProcessor = new StagingProcessor(
          targetTableFullName = s"iceberg.test.$targetName",
          icebergCatalogSettings = defaultIcebergStagingSettings,
          catalogWriter = writer,
          batchFactory = new BackfillMergeTestBatchFactory(),
          declaredMetrics = DeclaredMetrics(),
          nameGenerator = nameGenerator
        ),
        mergeProcessor = new MergeBatchProcessor(
          mergeServiceClient = mergeService,
          declaredMetrics = DeclaredMetrics()
        ),
        watermarkProcessor = new WatermarkProcessor(
          propertyManager = sinkPropertyManager,
          targetTableShortName = targetName,
          declaredMetrics = DeclaredMetrics()
        ),
        schemaMigrationProcessor = schemaMigrationProcessor
      )
    )
  yield (builder, backfillTableName)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("DefaultBackfillMergeGraphBuilderTests")(
    test("performs backfill merge by streaming exactly one changeset") {
      for
        rows <- ZIO.succeed(
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
        (builder, backfillTableName) <- runBackfill("test_backfill_merge", "backfill-merge-new", rows, streamSchema)
        result                       <- builder.produce().runCollect
      yield assertTrue(result.size == 1)
    }
  ).provide(
    writerLayer,
    VoidSchemaMigrationProcessor.layer,
    icebergUtilBackfill.getSinkEntityManagerLayer,
    icebergUtilBackfill.getStagingTablePropertyManagerLayer,
    icebergUtilBackfill.getStagingEntityManagerLayer,
    icebergUtilBackfill.getSinkTablePropertyManagerLayer
  ) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock
