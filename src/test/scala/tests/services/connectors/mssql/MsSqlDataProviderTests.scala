package com.sneaksanddata.arcane.framework
package tests.services.connectors.mssql

import models.app.StreamContext
import models.settings.VersionedDataGraphBuilderSettings
import services.mssql.*
import services.mssql.base.{ColumnSummary, ConnectionOptions, MsSqlReader, MsSqlServerFieldsFilteringService}
import tests.services.connectors.mssql.util.MsSqlTestServices.*
import tests.shared.TestStreamLifetimeService

import org.scalatest.matchers.should.Matchers.*
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Scope, Task, Unsafe, ZIO}

import java.sql.Connection
import java.time.Duration
import scala.language.postfixOps
import scala.util.Success

object MsSqlDataProviderTests extends ZIOSpecDefault:
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  private val fieldString = "(x int not null, y int)"
  private val pkString    = "primary key(x)"

  private val settings = new VersionedDataGraphBuilderSettings {
    override val lookBackInterval: Duration      = Duration.ofHours(1)
    override val changeCaptureInterval: Duration = Duration.ofMillis(1)
  }

  private val emptyFieldsFilteringService: MsSqlServerFieldsFilteringService = (fields: List[ColumnSummary]) =>
    Success(fields)

  private val streamContext = new StreamContext:
    override val IsBackfilling = false

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
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO
              .attemptBlocking(createTable("streaming_test", connection, fieldString, pkString))
              .flatMap(_ => insertData(connection, "streaming_test"))
        )
        connection <- ZIO.succeed(
          MsSqlReader(
            ConnectionOptions(connectionUrl, "dbo", "streaming_test", None),
            emptyFieldsFilteringService
          )
        )
        numberRowsToTake = 5
        provider              <- ZIO.succeed(MsSqlDataProvider(connection))
        streamingDataProvider <- ZIO.succeed(MsSqlStreamingDataProvider(provider, settings, streamContext))
        lifetimeService       <- ZIO.succeed(TestStreamLifetimeService(numberRowsToTake))
        rows                  <- streamingDataProvider.stream.takeWhile(_ => !lifetimeService.cancelled).runCollect
      yield assertTrue(rows.size == numberRowsToTake)
    }
  } @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
