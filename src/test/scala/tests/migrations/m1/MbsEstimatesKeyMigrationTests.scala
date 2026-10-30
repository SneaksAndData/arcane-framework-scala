package com.sneaksanddata.arcane.framework
package tests.migrations.m1

import migrations.m1.MbsEstimatesKeyMigration
import models.app.PluginStreamContext
import models.ddl.CreateTableRequest
import models.settings.sink.SinkSettings
import services.iceberg.base.SinkPropertyManager
import services.iceberg.{IcebergSinkEntityManager, IcebergSinkTablePropertyManager}
import services.merging.JdbcMergeServiceClient
import services.metrics.DeclaredMetrics
import services.streaming.throughput.MemoryBoundShaper
import tests.shared.{
  IcebergCatalogInfo,
  IcebergUtil,
  TestDynamicSinkSettings,
  TestJdbcMergeServiceClientSettings,
  TestPluginStreamContextImpl,
  TestStreamingSource
}

import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.types.Types
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO}

import java.util.UUID

object MbsEstimatesKeyMigrationTests extends ZIOSpecDefault:

  private val tableSchema = new IcebergSchema(
    Types.NestedField.optional(1, "id", Types.StringType.get())
  )

  private val testMergeServiceClient = new JdbcMergeServiceClient(
    TestJdbcMergeServiceClientSettings,
    "iceberg",
    "test",
    DeclaredMetrics(),
    false
  )

  private def withProvisionedTable[R, A](tableName: String)(
      testLogic: (IcebergSinkEntityManager, IcebergSinkTablePropertyManager, PluginStreamContext) => ZIO[
        R,
        Throwable,
        A
      ]
  ): ZIO[R & Scope, Throwable, A] =
    val icebergUtil = IcebergUtil(IcebergCatalogInfo.defaultSinkSettings)
    for
      entityManager   <- icebergUtil.getSinkEntityManager
      propertyManager <- icebergUtil.getSinkTablePropertyManager
      context = new TestPluginStreamContextImpl:
        override val sink: SinkSettings = new TestDynamicSinkSettings(s"demo.test.$tableName")
      result <- ZIO.acquireReleaseWith(
        entityManager.createTable(CreateTableRequest(tableName, tableSchema, true))
      )(_ => entityManager.delete(tableName).orDie) { _ =>
        testLogic(entityManager, propertyManager, context)
      }
    yield result

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("MbsEstimatesKeyMigration")(
    test("migrates legacy mbs-estimates property to arcane.core.throughput.mbs.estimates on provisioned table") {
      val tableName = s"mbs_mig_${UUID.randomUUID().toString.replace("-", "")}"
      val dummyEstimates =
        """{"recordCount":100,"physicalSize":1024,"rowSize":50,"partitions":2,"lastUpdate":"2026-10-28T12:00:00Z"}"""
      val migration = new MbsEstimatesKeyMigration()

      withProvisionedTable(tableName) { (_, propertyManager, context) =>
        for
          _ <- propertyManager.setProperty(tableName, "mbs-estimates", dummyEstimates)
          _ <- migration.execute(propertyManager, context, testMergeServiceClient, TestStreamingSource)
          newEstimates <- propertyManager.getRequiredProperty(
            tableName,
            MemoryBoundShaper.estimatePropertyKey
          )
          legacyProperty <- propertyManager.getProperty(
            tableName,
            "mbs-estimates"
          )
        yield assertTrue(newEstimates == dummyEstimates, legacyProperty.isEmpty)
      }
    },
    test("does not set property when legacy mbs-estimates is not present on provisioned table") {
      val tableName = s"mbs_mig_none_${UUID.randomUUID().toString.replace("-", "")}"
      val migration = new MbsEstimatesKeyMigration()

      withProvisionedTable(tableName) { (_, propertyManager, context) =>
        for
          _ <- migration.execute(propertyManager, context, testMergeServiceClient, TestStreamingSource)
          newEstimates <- propertyManager.getProperty(
            tableName,
            MemoryBoundShaper.estimatePropertyKey
          )
        yield assertTrue(newEstimates.isEmpty)
      }
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
