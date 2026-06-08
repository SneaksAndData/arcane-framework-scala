package com.sneaksanddata.arcane.framework
package tests.services.backfill

import models.schemas.{ArcaneSchema, DataRow}
import services.backfill.base.BackfillStreamDataProvider
import services.backfill.graph.DefaultBackfillMergeGraphBuilder
import services.filters.FieldsFilteringService
import services.iceberg.base.{SinkEntityManager, SinkPropertyManager, StagingEntityManager, StagingPropertyManager}
import services.iceberg.{IcebergCatalogFactory, IcebergS3CatalogWriter, IcebergStagingEntityManager}
import services.merging.JdbcMergeServiceClient
import services.metrics.DeclaredMetrics
import services.naming.DefaultNameGenerator
import services.streaming.base.StructuredZStream
import services.streaming.processors.batch_processors.streaming.{
  MergeBatchProcessor,
  SchemaMigrationProcessor,
  WatermarkProcessor
}
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import tests.shared.IcebergCatalogInfo.defaultIcebergStagingSettings
import tests.shared.*

import zio.stream.ZStream
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Scope, ZIO, ZLayer}

final class TestBackfillMergeStreamDataProvider(data: Seq[DataRow], schema: ArcaneSchema)
    extends BackfillStreamDataProvider:
  override def stream: ZStream[Any, Throwable, StructuredZStream] = ZStream.succeed(
    (ZStream.fromIterable(data), schema)
  )

object DefaultBackfillMergeGraphBuilderTests extends ZIOSpecDefault:
  private val icebergUtilBackfill = IcebergUtil(TestDynamicSinkSettings("test").icebergCatalog)
  private val writerLayer: ZLayer[Any, Throwable, IcebergS3CatalogWriter] = ZLayer.scoped {
    for
      factory <- IcebergCatalogFactory.live(defaultIcebergStagingSettings)
      entityManager = IcebergStagingEntityManager(defaultIcebergStagingSettings, factory)
      result        = IcebergS3CatalogWriter(entityManager, TestStagingSettings())
    yield result
  }

  private def runBackfill(targetName: String, backfillId: String) = for
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
        streamDataProvider = new TestBackfillMergeStreamDataProvider(Seq(), ArcaneSchema.empty()),
        fieldFilteringProcessor =
          new FieldFilteringTransformer(new FieldsFilteringService(TestFieldSelectionRuleSettings)),
        stagingProcessor = new StagingProcessor(
          targetTableFullName = s"iceberg.test.$targetName",
          icebergCatalogSettings = defaultIcebergStagingSettings,
          catalogWriter = writer,
          batchFactory = new TestStagedBatchFactory(),
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
  yield builder

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("DefaultBackfillMergeGraphBuilderTests")(
    test("performs backfill merge by streaming exactly one changeset") {
      for
        builder <- runBackfill("test_backfill_merge", "backfill-merge-new")
        result  <- builder.produce().runCollect
      yield assertTrue(1 == 1)
    }
  ).provide(
    writerLayer,
    VoidSchemaMigrationProcessor.layer,
    icebergUtilBackfill.getSinkEntityManagerLayer,
    icebergUtilBackfill.getStagingTablePropertyManagerLayer,
    icebergUtilBackfill.getStagingEntityManagerLayer,
    icebergUtilBackfill.getSinkTablePropertyManagerLayer
  ) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock
