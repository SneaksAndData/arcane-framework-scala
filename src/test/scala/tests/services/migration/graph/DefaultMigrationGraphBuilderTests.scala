package com.sneaksanddata.arcane.framework
package tests.services.migration.graph

import migrations.base.CurrentMigration
import models.MetadataKeys
import models.app.PluginStreamContext
import models.ddl.CreateTableRequest
import models.settings.sink.SinkSettings
import services.iceberg.base.SinkPropertyManager
import services.iceberg.{IcebergSinkEntityManager, IcebergSinkTablePropertyManager}
import services.merging.JdbcMergeServiceClient
import services.metrics.DeclaredMetrics
import services.migration.graph.DefaultMigrationGraphBuilder
import services.migration.{DefaultMigrationService, MigrationService}
import services.streaming.base.TimestampOnlyWatermark
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
import zio.{Scope, ZIO, ZLayer}

import java.time.OffsetDateTime
import java.util.UUID

object DefaultMigrationGraphBuilderTests extends ZIOSpecDefault:

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

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("DefaultMigrationGraphBuilderTests")(
    test("executes pending migrations and updates table properties when legacy property is present") {
      val tableName       = s"test_${UUID.randomUUID().toString.replace("-", "")}"
      val legacyWatermark = TimestampOnlyWatermark(OffsetDateTime.now())

      withProvisionedTable(tableName) { (_, propertyManager, context) =>
        for
          _ <- propertyManager.setProperty(tableName, "arcane-watermark", legacyWatermark.toJson)
          migrationService = new DefaultMigrationService(
            propertyManager,
            context,
            testMergeServiceClient,
            TestStreamingSource
          )
          builder = new DefaultMigrationGraphBuilder(migrationService)
          result <- builder.produce().runCollect
          newWatermark <- propertyManager.getRequiredProperty(
            tableName,
            MetadataKeys.getFrameworkKey(MetadataKeys.watermarkKey)
          )
          legacyWatermarkProperty <- propertyManager.getProperty(
            tableName,
            "arcane-watermark"
          )
        yield assertTrue(
          result.size == 1,
          newWatermark == legacyWatermark.toJson,
          legacyWatermarkProperty.isEmpty
        )
      }
    },
    test("does not execute migrations when table is already at current migration") {
      val tableName       = s"test_${UUID.randomUUID().toString.replace("-", "")}"
      val legacyWatermark = TimestampOnlyWatermark(OffsetDateTime.now())

      withProvisionedTable(tableName) { (_, propertyManager, context) =>
        for
          _ <- propertyManager.setProperty(
            tableName,
            MetadataKeys.getFrameworkKey(MetadataKeys.migrationKey),
            CurrentMigration.toString
          )
          _ <- propertyManager.setProperty(tableName, "arcane-watermark", legacyWatermark.toJson)
          migrationService = new DefaultMigrationService(
            propertyManager,
            context,
            testMergeServiceClient,
            TestStreamingSource
          )
          builder = new DefaultMigrationGraphBuilder(migrationService)
          result <- builder.produce().runCollect
          newWatermark <- propertyManager.getProperty(
            tableName,
            MetadataKeys.getFrameworkKey(MetadataKeys.watermarkKey)
          )
          legacyWatermarkProperty <- propertyManager.getRequiredProperty(
            tableName,
            "arcane-watermark"
          )
        yield assertTrue(
          result.isEmpty,
          newWatermark.isEmpty,
          legacyWatermarkProperty == legacyWatermark.toJson
        )
      }
    },
    test("runs migration stream without modifications when legacy property is absent") {
      val tableName = s"test_${UUID.randomUUID().toString.replace("-", "")}"

      withProvisionedTable(tableName) { (_, propertyManager, context) =>
        for
          migrationService = new DefaultMigrationService(
            propertyManager,
            context,
            testMergeServiceClient,
            TestStreamingSource
          )
          builder = new DefaultMigrationGraphBuilder(migrationService)
          result <- builder.produce().runCollect
          newWatermark <- propertyManager.getProperty(
            tableName,
            MetadataKeys.getFrameworkKey(MetadataKeys.watermarkKey)
          )
          legacyWatermarkProperty <- propertyManager.getProperty(
            tableName,
            "arcane-watermark"
          )
        yield assertTrue(
          result.size == 1,
          newWatermark.isEmpty,
          legacyWatermarkProperty.isEmpty
        )
      }
    },
    test("constructs and executes migration graph builder using ZLayer") {
      val tableName       = s"test_${UUID.randomUUID().toString.replace("-", "")}"
      val legacyWatermark = TimestampOnlyWatermark(OffsetDateTime.now())

      withProvisionedTable(tableName) { (_, propertyManager, context) =>
        for
          _ <- propertyManager.setProperty(tableName, "arcane-watermark", legacyWatermark.toJson)
          builder <- ZIO
            .service[DefaultMigrationGraphBuilder]
            .provide(
              DefaultMigrationGraphBuilder.layer,
              ZLayer.succeed[MigrationService](
                new DefaultMigrationService(propertyManager, context, testMergeServiceClient, TestStreamingSource)
              )
            )
          result <- builder.produce().runCollect
          newWatermark <- propertyManager.getRequiredProperty(
            tableName,
            MetadataKeys.getFrameworkKey(MetadataKeys.watermarkKey)
          )
          legacyWatermarkProperty <- propertyManager.getProperty(
            tableName,
            "arcane-watermark"
          )
        yield assertTrue(
          result.size == 1,
          newWatermark == legacyWatermark.toJson,
          legacyWatermarkProperty.isEmpty
        )
      }
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
