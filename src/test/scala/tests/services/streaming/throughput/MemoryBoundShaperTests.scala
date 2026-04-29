package com.sneaksanddata.arcane.framework
package tests.services.streaming.throughput

import models.ddl.CreateTableRequest
import models.schemas.{ArcaneSchema, MergeKeyField}
import models.settings.streaming.{MemoryBound, MemoryBoundImpl, ThroughputSettings, ThroughputShaperImpl}
import services.iceberg.base.SinkPropertyManager
import services.iceberg.given_Conversion_ArcaneSchema_Schema
import services.metrics.DeclaredMetrics
import services.streaming.throughput.MemoryBoundShaper
import tests.shared.{IcebergUtil, NullDimensionsProvider, TestDynamicSinkSettings}

import zio.test.TestAspect.timeout
import zio.test.*
import zio.{Scope, ZIO}

import java.time.Duration

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

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("MemoryBoundShaperTests")(
    test("correctly estimate on empty target") {
      for
        tableName <- ZIO.succeed("mbs_empty_table")
        settings <- ZIO.succeed(
          IcebergUtil(
            TestDynamicSinkSettings(s"iceberg.test.$tableName").icebergCatalog
          )
        )
        propertyManager <- settings.getSinkTablePropertyManager
        entityManager   <- settings.getSinkEntityManager

        _ <- entityManager.createTable(CreateTableRequest(tableName, ArcaneSchema(Seq(MergeKeyField)), true))

        shaper           <- ZIO.succeed(getShaper(tableName, propertyManager))
        chunkSize        <- shaper.estimateChunkSize
        currentMemory    <- ZIO.succeed(javaRuntime.maxMemory() - javaRuntime.totalMemory() + javaRuntime.freeMemory())
        expectedRowSize  <- ZIO.succeed(stringSize * 2 + 32 + 16)
        expectedElements <- ZIO.succeed(0.5 * currentMemory / (expectedRowSize + 1) / 2)
      yield assertTrue((chunkSize.Elements - expectedElements).abs < 1 && chunkSize.ElementSize == expectedRowSize)
    }
  ) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock
