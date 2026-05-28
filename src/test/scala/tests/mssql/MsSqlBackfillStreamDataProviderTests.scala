package com.sneaksanddata.arcane.framework
package tests.mssql

import logging.ZIOLogAnnotations.zlog
import models.backfill.DefaultSourceBackfill
import models.settings.TableNaming.{getBackfillTableName, parts}
import models.settings.backfill.BackfillBehavior.Overwrite
import models.settings.backfill.{BackfillBehavior, BackfillSettings}
import models.settings.mssql.MsSqlServerDatabaseSourceSettings
import models.settings.sources.{BufferingStrategy, SourceBufferingSettings, Unbounded, UnboundedImpl}
import services.backfill.DefaultBackfillStateManager
import services.metrics.DeclaredMetrics
import services.mssql.backfill.{MsSqlBackfillSourceDataProvider, MsSqlBackfillStreamDataProvider, MsSqlShardFactory}
import services.mssql.base.{ColumnSummary, MsSqlReader, MsSqlServerFieldsFilteringService}
import services.mssql.versioning.MsSqlWatermark
import tests.mssql.util.MsSqlTestServices
import tests.mssql.util.MsSqlTestServices.{createTable, getConnection}
import tests.shared.{IcebergUtil, TestDynamicSinkSettings, TestThroughputShaperBuilder}

import com.sneaksanddata.arcane.framework.models.schemas.ArcaneType.StringType
import com.sneaksanddata.arcane.framework.models.schemas.{ArcaneSchema, IndexedField, IndexedMergeKeyField}
import zio.stream.ZStream
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Scope, Task, ZIO}

import java.sql.Connection
import java.time.OffsetDateTime
import scala.util.{Random, Success}

object MsSqlBackfillStreamDataProviderTests extends ZIOSpecDefault:
  private val icebergUtilBackfill = IcebergUtil(TestDynamicSinkSettings("test").icebergCatalog)
  private val fieldString =
    val base = (1 to 50).map(ix => s"col$ix nvarchar(50)").mkString(",")
    s"(x int not null, $base)"
  private val pkString = "primary key(x)"
  private val emptyFieldsFilteringService: MsSqlServerFieldsFilteringService = (fields: List[ColumnSummary]) =>
    Success(fields)
  private val backfillSettings = new BackfillSettings {
    override val backfillStartDate: Option[OffsetDateTime] = None
    override val backfillBehavior: BackfillBehavior = Overwrite
  }
  private val targetSchema: ArcaneSchema = ArcaneSchema(
    Seq(IndexedMergeKeyField(0)) ++ (1 to 50).map(ix => IndexedField(
      name = s"col$ix",
      fieldType = StringType,
      fieldId = ix
    ))
  )

  private def prepareSourceTable(con: Connection, tableName: String, rowCount: Int = 10000): Task[Unit] =
    for
      _ <- zlog(s"Preparing test table with %s, be patient", rowCount.toString)
      _ <- ZIO.attemptBlocking(createTable(tableName, con, fieldString, pkString))

      _ <- ZStream.fromIterable(1 to rowCount).mapZIOPar(1024) { index =>
        ZIO.acquireReleaseWith(ZIO.attempt(con.createStatement()))(statement =>
          ZIO.attemptBlocking(statement.close()).orDie
        ) { statement =>
            val colValues = (1 to 50).map(_ => Random.alphanumeric.take(Random.nextInt(48) + 1).mkString).map(v => s"N'$v'").mkString(",")
            val insertCmd =
              s"use arcane; insert into dbo.$tableName values($index, $colValues)"
            ZIO.attemptBlocking(statement.execute(insertCmd))
          }
        }.runDrain
    yield ()

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("MsSqlBackfillStreamDataProviderTests")(
    test("streams correct number of shards and rows") {
      for
        testTableName <- ZIO.succeed("backfill_test_1")
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection => prepareSourceTable(connection, testTableName)
        )
        tableSinkSettings   <- ZIO.succeed(TestDynamicSinkSettings("iceberg.test.mssql_new_backfill"))
        icebergUtilBackfill <- ZIO.succeed(IcebergUtil(tableSinkSettings.icebergCatalog))

        _ <- icebergUtilBackfill.prepareBackfillTable(
          getBackfillTableName("mssql_backfill_new"), targetSchema
        )
        propertyManager        <- icebergUtilBackfill.getSinkTablePropertyManager
        stagingPropertyManager <- icebergUtilBackfill.getStagingTablePropertyManager
        stagingEntityManager   <- icebergUtilBackfill.getStagingEntityManager
        backfillStateManager <- ZIO.succeed(
          new DefaultBackfillStateManager(
            stagingEntityManager,
            stagingPropertyManager,
            new MsSqlShardFactory(),
            getBackfillTableName("mssql_backfill_new")
          )
        )
        shaperBuilder <- ZIO.succeed(
          TestThroughputShaperBuilder.default(propertyManager, tableSinkSettings)
        )
        reader <- ZIO.succeed(
          MsSqlReader(
            new MsSqlServerDatabaseSourceSettings {
              override val connectionUrl: String                          = MsSqlTestServices.connectionUrl
              override val schemaName: String                             = "dbo"
              override val tableName: String                              = testTableName
              override val fetchSize: Option[Int]                         = None
              override val extraConnectionParameters: Map[String, String] = Map.empty
              override val shardSizeMegabytes: Option[Int]                = None
              override val backfillShardSchemaName: String                = "dbo"
            },
            emptyFieldsFilteringService
          )
        )
        dataProvider <- ZIO.succeed(
          new MsSqlBackfillSourceDataProvider(
            reader,
            backfillSettings,
            tableSinkSettings,
            backfillStateManager,
            shaperBuilder,
            new SourceBufferingSettings {
              override val bufferingStrategy: BufferingStrategy = UnboundedImpl(Unbounded())
              override val bufferingEnabled: Boolean = false
            },
            "mssql_backfill_new"
          )
        )
        provider <- ZIO.succeed(
          new MsSqlBackfillStreamDataProvider(
            dataProvider,
            backfillSettings,
            backfillStateManager,
            DeclaredMetrics()
          )
        )
        data      <- provider.backfillStream
        shards    <- data.stream.runCollect
        shardRows <- ZStream.fromIterable(shards).flatMap(_.shardStream._1).runCount
        backfillState <- stagingPropertyManager
          .getRequiredProperty(getBackfillTableName("mssql_backfill_new"), "backfill")
          .map(upickle.read[DefaultSourceBackfill](_))
      yield assertTrue(shards.size == 10 && shardRows == 1000 && backfillState.id == "mssql_backfill_new")
    }
  ) @@ timeout(zio.Duration.fromSeconds(180)) @@ TestAspect.withLiveClock
