package com.sneaksanddata.arcane.framework
package tests.mssql

import models.app.StreamContext
import models.schemas.ArcaneType.StringType
import models.schemas.{ArcaneSchema, Field}
import models.settings.VersionedDataGraphBuilderSettings
import models.settings.backfill.BackfillBehavior.Overwrite
import models.settings.backfill.{BackfillBehavior, BackfillSettings}
import services.iceberg.{IcebergS3CatalogWriter, IcebergTablePropertyManager, given_Conversion_ArcaneSchema_Schema}
import services.metrics.DeclaredMetrics
import services.mssql.*
import services.mssql.base.{ColumnSummary, ConnectionOptions, MsSqlReader, MsSqlServerFieldsFilteringService}
import services.mssql.versioning.MsSqlWatermark
import tests.mssql.util.MsSqlTestServices.{connectionUrl, createTable, getConnection}
import tests.shared.IcebergCatalogInfo.defaultStagingSettings
import tests.shared.{NullDimensionsProvider, TestDynamicSinkSettings, TestStreamLifetimeService}

import org.scalatest.matchers.should.Matchers.*
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Scope, Task, Unsafe, ZIO}

import java.sql.Connection
import java.time.{Duration, OffsetDateTime, ZoneOffset}
import scala.language.postfixOps
import scala.util.Success

object MsSqlDataProviderTests extends ZIOSpecDefault:
  private val graphSettings = new VersionedDataGraphBuilderSettings {
    override val changeCaptureInterval: Duration = Duration.ofSeconds(5)
  }
  private val backfillSettings = new BackfillSettings {
    override val backfillBehavior: BackfillBehavior = Overwrite
    override val backfillStartDate: Option[OffsetDateTime] = Some(
      OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofHours(12))
    )
    override val backfillTableFullName: String = "backfill_test"
  }

  private val fieldString = "(x int not null, y int)"
  private val pkString    = "primary key(x)"

  private val settings = new VersionedDataGraphBuilderSettings {
    override val changeCaptureInterval: Duration = Duration.ofMillis(1)
  }

  private val emptyFieldsFilteringService: MsSqlServerFieldsFilteringService = (fields: List[ColumnSummary]) =>
    Success(fields)

  private val streamContext = new StreamContext:
    override val IsBackfilling = false

  private val propertyManager: IcebergTablePropertyManager = IcebergTablePropertyManager(
    TestDynamicSinkSettings(backfillSettings.backfillTableFullName)
  )
  private val writer: IcebergS3CatalogWriter = IcebergS3CatalogWriter(defaultStagingSettings)

  private def prepareWatermark(tableName: String, provider: MsSqlDataProvider): Task[Unit] =
    for
      _ <- writer.createTable(tableName, ArcaneSchema(Seq(Field("test", StringType))), true)
      _ <- propertyManager.comment(tableName, MsSqlWatermark.epoch.toJson)
    yield ()

  def insertData(con: Connection, tableName: String): Task[Unit] =
    for
      _ <- ZIO.acquireReleaseWith(ZIO.attempt(con.createStatement()))(statement =>
        ZIO.attemptBlocking(statement.close()).orDie
      ) { statement =>
        ZIO.foreach(1 to 10) { index =>
          val insertCmd =
            s"use arcane; insert into dbo.$tableName values($index, ${index + 1})"
          ZIO.attemptBlocking(statement.execute(insertCmd))
        }
      }
      _ <- ZIO.acquireReleaseWith(ZIO.attempt(con.createStatement()))(statement =>
        ZIO.attemptBlocking(statement.close()).orDie
      ) { statement =>
        ZIO.foreach(1 to 10) { index =>
          val updateCmd =
            s"use arcane; insert into dbo.$tableName values(${index * 1000}, ${index * 1000 + 1})"
          ZIO.attemptBlocking(statement.execute(updateCmd))
        }
      }
    yield ()

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("MsSqlDataProviderTests") {
    test("returns correct number of rows while streaming") {
      for
        tableName <- ZIO.succeed("streaming_test")
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO
              .attemptBlocking(createTable(tableName, connection, fieldString, pkString))
              .flatMap(_ => insertData(connection, tableName))
        )
        connection <- ZIO.succeed(
          MsSqlReader(
            ConnectionOptions(connectionUrl, "dbo", tableName, None),
            emptyFieldsFilteringService
          )
        )
        numberRowsToTake =
          5 // if set to 20, will run indefinitely since no elements will be emitted and cancelled will not be called
        provider <- ZIO.succeed(
          MsSqlDataProvider(
            connection,
            propertyManager,
            new TestDynamicSinkSettings(s"demo.test.$tableName"),
            graphSettings,
            backfillSettings
          )
        )
        _ <- prepareWatermark(tableName, provider)
        streamingDataProvider <- ZIO.succeed(
          MsSqlStreamingDataProvider(
            provider,
            settings,
            backfillSettings,
            streamContext,
            DeclaredMetrics(NullDimensionsProvider)
          )
        )
        lifetimeService <- ZIO.succeed(TestStreamLifetimeService(numberRowsToTake))
        rows            <- streamingDataProvider.stream.rechunk(1).takeUntil(_ => lifetimeService.cancelled).runCollect
      yield assertTrue(rows.size == numberRowsToTake)
    }
  } @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
