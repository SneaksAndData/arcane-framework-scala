package com.sneaksanddata.arcane.framework
package tests.services.backfill

import models.queries.StreamingBatchQuery
import models.schemas.*
import models.schemas.ArcaneType.{IntType, StringType}
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
import tests.shared.*
import tests.shared.IcebergCatalogInfo.defaultIcebergStagingSettings
import tests.shared.TestTrinoConnection.newTrinoConnection

import zio.stream.ZStream
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Schedule, Scope, Task, ZIO, ZLayer}

import java.time.OffsetDateTime

final class TestBackfillStreamDataProvider(targetName: String, shards: Seq[BootstrappedShard])
    extends BackfillStreamDataProvider:
  override def backfillStream: Task[(stream: ZStream[Any, Throwable, BootstrappedShard], watermark: JsonWatermark)] =
    ZIO.succeed(
      (
        stream = ZStream.fromIterable(shards),
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
        targetName <- ZIO.succeed("iceberg.test.generic_stream")
        shards <- ZIO.succeed(
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
                streamSchema
              ),
              "shard1",
              "backfill__generic__test_combined",
              targetName,
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
                streamSchema
              ),
              "shard2",
              "backfill__generic__test_combined",
              targetName,
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
                streamSchema
              ),
              "shard3",
              "backfill__generic__test_combined",
              targetName,
              "test_combined"
            )
          )
        )
        trinoConnection <- newTrinoConnection
        writer          <- ZIO.service[IcebergS3CatalogWriter]
        mergeService <- ZIO.succeed(
          new JdbcMergeServiceClient(
            TestJdbcMergeServiceClientSettings,
            "iceberg",
            "test",
            DeclaredMetrics(),
            true
          )
        )
        // backfill table should exist
        _ <- icebergUtilBackfill.prepareBackfillTable(
          getBackfillTableName("generic__test_combined"),
          streamSchema,
          recreate = false
        )
        shardFactory           <- ZIO.succeed(new TestShardFactory())
        propertyManager        <- ZIO.service[SinkPropertyManager]
        stagingPropertyManager <- ZIO.service[StagingPropertyManager]
        stagingEntityManager   <- ZIO.service[StagingEntityManager]
        backfillStateManager <- ZIO.succeed(
          new DefaultBackfillStateManager(
            stagingEntityManager,
            stagingPropertyManager,
            new TestShardFactory(),
            getBackfillTableName("generic__test_combined")
          )
        )
        builder <- ZIO.succeed(
          DefaultBackfillOverwriteGraphBuilder(
            new TestBackfillStreamDataProvider(targetName, shards),
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
        result               <- builder.produce().runCollect
        expectedRowsInTarget <- ZStream.fromIterable(shards).flatMap(_.shardStream._1).runCount
        // final table must have num rows matching num shards since shard aggregate is 1:1 to target
        rowsInTarget <- ZIO.scoped {
          ZIO
            .fromAutoCloseable(ZIO.attempt(trinoConnection.prepareStatement(s"select count (1) from $targetName")))
            .map { st =>
              val rs = st.executeQuery()
              if rs.next() then rs.getInt(1)
              else 0
            }
        }
      yield assertTrue(result.toList match
        case (completedShard: CompletedShard) :: nil =>
          completedShard.targetTableName == "iceberg.test.generic_stream" && completedShard.combinedTableName == getBackfillTableName(
            "generic__test_combined"
          ) && rowsInTarget == expectedRowsInTarget
        case _ => false)
    } @@ TestAspect.repeat(Schedule.recurs(2)) // this test must run initial backfill and then produce the same result!
  ).provide(
    writerLayer,
    icebergUtilBackfill.getStagingTablePropertyManagerLayer,
    icebergUtilBackfill.getStagingEntityManagerLayer,
    icebergUtilBackfill.getSinkTablePropertyManagerLayer
  ) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock
