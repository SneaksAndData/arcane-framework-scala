package com.sneaksanddata.arcane.framework
package tests.mssql

import models.app.BaseStreamContext
import models.settings.backfill.BackfillBehavior.Overwrite
import models.settings.backfill.{BackfillBehavior, BackfillSettings}
import models.settings.mssql.MsSqlServerDatabaseSourceSettings
import models.settings.streaming.{ChangeCaptureSettings, StreamModeSettings}
import services.metrics.DeclaredMetrics
import services.mssql.*
import services.mssql.base.MsSqlStreamingSource
import services.mssql.versioning.MsSqlWatermark
import services.naming.DefaultNameGenerator
import tests.mssql.util.MsSqlTestServices
import tests.mssql.util.MsSqlTestServices.{createTable, getConnection}
import tests.shared.*

import com.sneaksanddata.arcane.framework.models.schemas.MergeKeyField
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Scope, Task, ZIO}

import java.sql.Connection
import java.time.{Duration, OffsetDateTime, ZoneOffset}
import scala.language.postfixOps

object MsSqlStreamingDataProviderTests extends ZIOSpecDefault:
  private val defaultStreamMode = new StreamModeSettings {

    /** Backfill mode-only settings
      */
    override val backfill: BackfillSettings = new BackfillSettings {
      override val backfillBehavior: BackfillBehavior = Overwrite
      override val backfillStartDate: Option[OffsetDateTime] = Some(
        OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofHours(12))
      )
    }

    /** Change capture mode settings
      */
    override val changeCapture: ChangeCaptureSettings = new ChangeCaptureSettings {
      override val changeCaptureInterval: Duration     = Duration.ofSeconds(1)
      override val changeCaptureJitterVariance: Double = 0.0001
      override val changeCaptureJitterSeed: Long       = 0
      override val changeCaptureRangeLimit: Int        = 1000
    }
  }

  private val stagingSettings = TestStagingSettings()

  private val fieldString = "(x int not null, y int)"
  private val pkString    = "primary key(x)"

  private val streamContext = new BaseStreamContext:
    override def isBackfilling: ZIO[Any, SecurityException, Boolean] = ZIO.succeed(false)

  private val defaultSinkSettings = TestDynamicSinkSettings("mssql__mssql_test")
  private val icebergUtil         = IcebergUtil(defaultSinkSettings.icebergCatalog)

  def insertData(con: Connection, tableName: String, rowsToInsert1: Int, rowsToInsert2: Int): Task[Unit] =
    for
      _ <- ZIO.acquireReleaseWith(ZIO.attempt(con.createStatement()))(statement =>
        ZIO.attemptBlocking(statement.close()).orDie
      ) { statement =>
        ZIO.foreach(1 to rowsToInsert1) { index =>
          val insertCmd =
            s"use arcane; insert into dbo.$tableName values($index, ${index + 1})"
          ZIO.attemptBlocking(statement.execute(insertCmd))
        }
      }
      _ <- ZIO.acquireReleaseWith(ZIO.attempt(con.createStatement()))(statement =>
        ZIO.attemptBlocking(statement.close()).orDie
      ) { statement =>
        ZIO.foreach(1 to rowsToInsert2) { index =>
          val updateCmd =
            s"use arcane; insert into dbo.$tableName values(${index * 1000}, ${index * 1000 + 1})"
          ZIO.attemptBlocking(statement.execute(updateCmd))
        }
      }
    yield ()

  private val nameGenerator =
    new DefaultNameGenerator(
      sinkSettings = TestSinkSettings,
      backfillId = "",
      streamId = "mssql_reader_tests"
    )

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("MsSqlStreamingDataProviderTests") {
    test("returns correct number of rows while streaming") {
      for
        testTableName <- ZIO.succeed("streaming_test")
        totalRowsToInsert = 20
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO
              .attemptBlocking(createTable(testTableName, connection, fieldString, pkString))
              .flatMap(_ =>
                insertData(
                  connection,
                  testTableName,
                  rowsToInsert1 = totalRowsToInsert / 2,
                  rowsToInsert2 = totalRowsToInsert / 2
                )
              )
        )
        connection <- ZIO.succeed(
          MsSqlStreamingSource(
            new MsSqlServerDatabaseSourceSettings {
              override val connectionUrl: String                          = MsSqlTestServices.connectionUrl
              override val schemaName: String                             = "dbo"
              override val tableName: String                              = testTableName
              override val fetchSize: Option[Int]                         = None
              override val extraConnectionParameters: Map[String, String] = Map.empty
              override val shardSizeMegabytes: Option[Int]                = None
              override val backfillShardSchemaName: String                = "dbo"
            },
            nameGenerator,
            Seq.empty
          )
        )
        propertyManager <- icebergUtil.getSinkTablePropertyManager
        provider <- ZIO.succeed(
          MsSqlDataProvider(
            connection,
            propertyManager,
            new TestDynamicSinkSettings(s"demo.test.$testTableName"),
            TestThroughputShaperBuilder.default(
              propertyManager,
              new TestDynamicSinkSettings(s"demo.test.$testTableName")
            ),
            TestSourceBufferingSettings,
            DeclaredMetrics()
          )
        )
        _ <- icebergUtil.prepareWatermark(testTableName, MsSqlWatermark.epoch)
        streamingDataProvider <- ZIO.succeed(
          MsSqlStreamingDataProvider(
            provider,
            defaultStreamMode.changeCapture,
            DeclaredMetrics()
          )
        )
        rows <- streamingDataProvider.stream
          .interruptAfter(zio.Duration.fromSeconds(2))
          .flatMap(_._1.haltAfter(zio.Duration.fromSeconds(2)))
          .rechunk(1)
          .runCollect
        watermarks = rows.filter(_.isWatermark)
        data       = rows.filterNot(_.isWatermark)
      yield assertTrue(
        data
          .map(_.filter(_.name == MergeKeyField.name).head.value.toString)
          .toSet
          .size == totalRowsToInsert && watermarks.nonEmpty
      )
    }
  } @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
