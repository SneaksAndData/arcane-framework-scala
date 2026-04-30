package com.sneaksanddata.arcane.framework
package tests.services.streaming.throughput

import models.ddl.CreateTableRequest
import models.schemas.{ArcaneSchema, MergeKeyField}
import models.settings.streaming.{MemoryBound, MemoryBoundImpl, ThroughputSettings, ThroughputShaperImpl}
import services.iceberg.base.SinkPropertyManager
import services.iceberg.{
  IcebergSinkEntityManager,
  IcebergSinkTablePropertyManager,
  given_Conversion_ArcaneSchema_Schema
}
import services.metrics.DeclaredMetrics
import services.streaming.throughput.MemoryBoundShaper
import tests.shared.{IcebergUtil, NullDimensionsProvider, TestDynamicSinkSettings}

import io.trino.jdbc.TrinoDriver
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, Task, ZIO}

import java.time.Duration
import java.util.Properties

object MemoryBoundShaperTests extends ZIOSpecDefault:
  private val stringSize: Int = 100
  private val javaRuntime     = Runtime.getRuntime

  private def getShaper(tableName: String, propertyManager: SinkPropertyManager) = MemoryBoundShaper(
    propertyManager = propertyManager,
    targetTableShortName = tableName,
    memoryBoundShaperSettings = new ThroughputSettings {
      override val shaperImpl: ThroughputShaperImpl =
        MemoryBoundImpl(MemoryBound(stringSize, 4096, 2, 2, 1, 10, 0.5, 0.5, 2, 5))
      override val advisedChunkSize: Int       = 10
      override val advisedRateChunks: Int      = 1
      override val advisedRatePeriod: Duration = Duration.ofSeconds(1)
      override val advisedChunksBurst: Int     = 10
    },
    declaredMetrics = DeclaredMetrics(NullDimensionsProvider)
  )

  private def getIcebergManagers(
      tableName: String
  ): ZIO[Scope, Throwable, (IcebergSinkTablePropertyManager, IcebergSinkEntityManager)] =
    for
      tableName <- ZIO.succeed("mbs_empty_table")
      settings <- ZIO.succeed(
        IcebergUtil(
          TestDynamicSinkSettings(s"iceberg.test.$tableName").icebergCatalog
        )
      )
      propertyManager <- settings.getSinkTablePropertyManager
      entityManager   <- settings.getSinkEntityManager
    yield (propertyManager, entityManager)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("MemoryBoundShaperTests")(
//    test("correctly estimate on empty target") {
//      for
//        tableName <- ZIO.succeed("mbs_empty_table")
//        (propertyManager, entityManager) <- getIcebergManagers(tableName)
//
//        _ <- entityManager.createTable(CreateTableRequest(tableName, ArcaneSchema(Seq(MergeKeyField)), true))
//
//        shaper           <- ZIO.succeed(getShaper(tableName, propertyManager))
//        chunkSize        <- shaper.estimateChunkSize
//        currentMemory    <- ZIO.succeed(javaRuntime.maxMemory() - javaRuntime.totalMemory() + javaRuntime.freeMemory())
//        expectedRowSize  <- ZIO.succeed(stringSize * 2 + 32 + 16)
//        expectedElements <- ZIO.succeed(0.5 * currentMemory / (expectedRowSize + 1) / 2)
//      yield assertTrue((chunkSize.Elements - expectedElements).abs < 5000 && chunkSize.ElementSize == expectedRowSize)
//    },
    test("correctly estimate on non-empty target") {
      for
        // prep iceberg
        tableName                        <- ZIO.succeed("mbs_non_empty_table")
        (propertyManager, entityManager) <- getIcebergManagers(tableName)

        // prep table
        driver <- ZIO.succeed(new TrinoDriver())
        connection <- ZIO.attemptBlocking(
          driver.connect("jdbc:trino://localhost:8080/iceberg/test?user=test", new Properties())
        )
        statement <- ZIO.attemptBlocking(connection.createStatement())
        createTableStatement =
          s"CREATE TABLE IF NOT EXISTS iceberg.test.$tableName (ARCANE_MERGE_KEY VARCHAR, colA VARCHAR, colB INT)"
        _ <- ZIO.attemptBlocking(statement.execute(createTableStatement))
        insertRowsStatement = s"""INSERT INTO iceberg.test.$tableName (ARCANE_MERGE_KEY, colA, colB)
                                |VALUES
                                |  ('KEY_CUOP_1', 'Value_VP', 694),
                                |  ('KEY_HOQ5_2', 'Value_U3', 702),
                                |  ('KEY_6TIY_3', 'Value_4F', 296),
                                |  ('KEY_G7Y6_4', 'Value_ZZ', 907),
                                |  ('KEY_JPFE_5', 'Value_DX', 864),
                                |  ('KEY_LWXU_6', 'Value_ND', 459),
                                |  ('KEY_TRJQ_7', 'Value_MN', 499),
                                |  ('KEY_IJPZ_8', 'Value_HU', 646),
                                |  ('KEY_UTCP_9', 'Value_DG', 437),
                                |  ('KEY_3H05_10', 'Value_YK', 226)""".stripMargin
        _ <- ZIO.attemptBlocking(statement.execute(insertRowsStatement))
        _ <- ZIO.attemptBlocking(statement.close())

        // check
        shaper    <- ZIO.succeed(getShaper(tableName, propertyManager))
        chunkSize <- shaper.estimateChunkSize
//        currentMemory    <- ZIO.succeed(javaRuntime.maxMemory() - javaRuntime.totalMemory() + javaRuntime.freeMemory())
//        expectedRowSize  <- ZIO.succeed(stringSize * 2 + 32 + 16)
//        expectedElements <- ZIO.succeed(0.5 * currentMemory / (expectedRowSize + 1) / 2)
      yield assertTrue(chunkSize.Elements == 1 && chunkSize.ElementSize == 1)
    }
  ) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock
