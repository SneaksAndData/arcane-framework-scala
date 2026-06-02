package com.sneaksanddata.arcane.framework
package tests.services.backfill

import models.queries.StreamingBatchQuery
import models.schemas.*
import models.schemas.ArcaneType.{IntType, StringType}
import models.sharding.*
import services.backfill.DefaultBackfillStateManager
import services.backfill.base.{BackfillStreamDataProvider, ShardFactory, ShardProcessingState}
import services.backfill.graph.DefaultBackfillOverwriteGraphBuilder
import services.backfill.processors.{BackfillCompletionProcessor, ShardStagingProcessor}
import services.filters.FieldsFilteringService
import services.iceberg.base.{SinkPropertyManager, StagingEntityManager, StagingPropertyManager}
import services.iceberg.{IcebergCatalogFactory, IcebergS3CatalogWriter, IcebergStagingEntityManager}
import services.merging.JdbcMergeServiceClient
import services.metrics.DeclaredMetrics
import services.naming.{DefaultNameGenerator, NameGenerator}
import services.streaming.base.{JsonWatermark, TimestampOnlyWatermark}
import services.streaming.processors.transformers.FieldFilteringTransformer
import tests.shared.*
import tests.shared.IcebergCatalogInfo.defaultIcebergStagingSettings
import tests.shared.TestTrinoConnection.newTrinoConnection

import zio.stream.ZStream
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Schedule, Scope, Task, ZIO, ZLayer}

import java.sql.Connection
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

final class TestShardFactory(nameGenerator: NameGenerator) extends ShardFactory:
  override def createStagedShard(shard: BootstrappedShard): Task[StagedShard] = nameGenerator.getShardTableName(shard).map(shardTableName => DefaultStagedShard(
    shard.shardSourceEntityName,
    shard.combinedTableName,
    shard.targetTableName,
    new StreamingBatchQuery {
      override def query: String = s"INSERT INTO ${shard.combinedTableName} SELECT * FROM ${shardTableName}"
    },
    shard.backfillId
  ))

  override def createCompletionShard(shard: StagedShard, watermark: String): Task[CompletionShard] = ZIO.succeed(CompletionShard(
    watermark = watermark,
    targetTableName = shard.targetTableName,
    shardSourceEntityName = shard.shardSourceEntityName,
    combinedTableName = shard.combinedTableName,
    commitQuery = new StreamingBatchQuery {
      override def query: String =
        s"CREATE OR REPLACE TABLE ${shard.targetTableName} AS SELECT * FROM ${shard.combinedTableName}"
    },
    shard.backfillId
  ))

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
  private def getRowsInTarget(con: Connection, tableName: String) = ZIO.scoped {
    ZIO
      .fromAutoCloseable(ZIO.attempt(con.prepareStatement(s"select count (1) from $tableName")))
      .map { st =>
        val rs = st.executeQuery()
        if rs.next() then rs.getInt(1)
        else 0
      }
  }
  private def getShards(targetName: String, backfillTableName: String, id: String) = Seq(
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
      backfillTableName,
      targetName,
      id
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
      backfillTableName,
      targetName,
      id
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
      backfillTableName,
      targetName,
      id
    )
  )

  private def runBackfill(targetName: String, backfillId: String) =
    for
      nameGenerator <- ZIO.succeed(new DefaultNameGenerator(
        sinkSettings = TestDynamicSinkSettings(targetName), backfillId = backfillId, streamId = "default-backfill-overwrite-graph-builder"
      ))
      backfillTableName <- nameGenerator.getBackfillTableName
      shards <- ZIO.succeed(getShards(targetName, backfillTableName, backfillId))
      shardTableNames <- ZStream.fromIterable(shards).mapZIO(nameGenerator.getShardTableName).runCollect.map(_.toList)
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
      // backfill table should exist
      _ <- icebergUtilBackfill.prepareBackfillTable(
        backfillTableName,
        streamSchema,
        recreate = false
      )
      shardFactory           <- ZIO.succeed(new TestShardFactory(nameGenerator))
      propertyManager        <- ZIO.service[SinkPropertyManager]
      stagingPropertyManager <- ZIO.service[StagingPropertyManager]
      stagingEntityManager   <- ZIO.service[StagingEntityManager]
      backfillStateManager <- ZIO.succeed(
        new DefaultBackfillStateManager(
          stagingEntityManager,
          stagingPropertyManager,
          shardFactory,
          nameGenerator
        )
      )
      builder <- ZIO.succeed(
        DefaultBackfillOverwriteGraphBuilder(
          new TestBackfillStreamDataProvider(targetName, shards),
          new ShardStagingProcessor(
            writer,
            shardFactory,
            nameGenerator,
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
    yield (result, shards, backfillTableName, shardTableNames)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("DefaultBackfillOverwriteGraphBuilderTests")(
    test("stages shards, aggregates them and swaps correct data into target table") {
      for
        trinoConnection <- newTrinoConnection
        // expect the following:
        // a single row - CompletedShard is the output
        (result, shards, backfillTableName, _) <- runBackfill(
          "iceberg.test.generic_stream",
          "test_combined"
        )
        expectedRowsInTarget <- ZStream.fromIterable(shards).flatMap(_.shardStream._1).runCount
        // final table must have num rows matching num shards since shard aggregate is 1:1 to target
        rowsInTarget <- getRowsInTarget(trinoConnection, "iceberg.test.generic_stream")
      yield assertTrue(result.toList match
        case (completedShard: CompletedShard) :: nil =>
          completedShard.targetTableName == "iceberg.test.generic_stream" && completedShard.combinedTableName == backfillTableName && rowsInTarget == expectedRowsInTarget
        case _ => false)
    } @@ TestAspect.repeat(Schedule.recurs(2)), // this test must run initial backfill and then produce the same result!
    test("picks up backfill when a shard has been staged") {
      for
        trinoConnection <- newTrinoConnection
        (_, _, backfillTableName, shardTableNames) <- runBackfill(
          "iceberg.test.interrupted_backfill_stream",
          "test_interrupted"
        )
        // now simulate interruption:
        // truncate target
        _ <- ZIO.scoped {
          ZIO
            .fromAutoCloseable(
              ZIO.attempt(trinoConnection.prepareStatement("truncate table iceberg.test.interrupted_backfill_stream"))
            )
            .map(_.execute())
        }
        // truncate combined
        _ <- ZIO.scoped {
          ZIO
            .fromAutoCloseable(
              ZIO.attempt(
                trinoConnection.prepareStatement(
                  s"truncate table iceberg.test.$backfillTableName"
                )
              )
            )
            .map(_.execute())
        }
        // drop shard2 and shard3 tables
        _ <- ZIO.scoped {
          ZIO
            .fromAutoCloseable(
              ZIO.attempt(
                trinoConnection.prepareStatement(s"drop table iceberg.test.${shardTableNames(1)}")
              )
            )
            .map(_.execute())
        }
        _ <- ZIO.scoped {
          ZIO
            .fromAutoCloseable(
              ZIO.attempt(
                trinoConnection.prepareStatement(s"drop table iceberg.test.${shardTableNames(2)}")
              )
            )
            .map(_.execute())
        }
        // update shard1 table back to Staged
        stagingPropertyManager <- ZIO.service[StagingPropertyManager]
        _ <- stagingPropertyManager.setProperty(
          "backfill_shard__test_interrupted__shard1",
          "processing-state",
          ShardProcessingState.STAGED.toString
        )
        // re-run and expect identical result to a full backfill
        (result, shards, backfillTableName, _) <- runBackfill(
          "iceberg.test.interrupted_backfill_stream",
          "test_interrupted"
        )
        expectedRowsInTarget <- ZStream.fromIterable(shards).flatMap(_.shardStream._1).runCount
        rowsInTarget         <- getRowsInTarget(trinoConnection, "iceberg.test.interrupted_backfill_stream")
      yield assertTrue(rowsInTarget == expectedRowsInTarget)
    },
    test("picks up backfill when a shard has been combined") {
      for
        trinoConnection <- newTrinoConnection
        (_, _, backfillTableName, shardTableNames) <- runBackfill(
          "iceberg.test.interrupted_1_backfill_stream",
          "test_interrupted_1"
        )
        // now simulate interruption:
        // truncate target
        _ <- ZIO.scoped {
          ZIO
            .fromAutoCloseable(
              ZIO.attempt(trinoConnection.prepareStatement("truncate table iceberg.test.interrupted_1_backfill_stream"))
            )
            .map(_.execute())
        }
        // delete from combined: shard2 and shard3 data
        _ <- ZIO.scoped {
          ZIO
            .fromAutoCloseable(
              ZIO.attempt(
                trinoConnection.prepareStatement(
                  s"delete from iceberg.test.$backfillTableName where colB > 2"
                )
              )
            )
            .map(_.execute())
        }
        // drop shard2 and shard3 tables
        _ <- ZIO.scoped {
          ZIO
            .fromAutoCloseable(
              ZIO.attempt(
                trinoConnection.prepareStatement(s"drop table iceberg.test.${shardTableNames(1)}")
              )
            )
            .map(_.execute())
        }
        _ <- ZIO.scoped {
          ZIO
            .fromAutoCloseable(
              ZIO.attempt(
                trinoConnection.prepareStatement(s"drop table iceberg.test.${shardTableNames(2)}")
              )
            )
            .map(_.execute())
        }
        // leave shard1 as COMBINED
        // re-run and expect identical result to a full backfill
        (result, shards, _, _) <- runBackfill(
          "iceberg.test.interrupted_1_backfill_stream",
          "test_interrupted_1"
        )
        expectedRowsInTarget <- ZStream.fromIterable(shards).flatMap(_.shardStream._1).runCount
        rowsInTarget         <- getRowsInTarget(trinoConnection, "iceberg.test.interrupted_1_backfill_stream")
      yield assertTrue(rowsInTarget == expectedRowsInTarget)
    }
  ).provide(
    writerLayer,
    icebergUtilBackfill.getStagingTablePropertyManagerLayer,
    icebergUtilBackfill.getStagingEntityManagerLayer,
    icebergUtilBackfill.getSinkTablePropertyManagerLayer
  ) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock
