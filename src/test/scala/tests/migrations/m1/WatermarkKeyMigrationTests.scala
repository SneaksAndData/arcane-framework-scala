package com.sneaksanddata.arcane.framework
package tests.migrations.m1

import migrations.m1.WatermarkKeyMigration
import models.MetadataKeys
import models.app.PluginStreamContext
import models.ddl.CreateTableRequest
import models.settings.sink.SinkSettings
import services.iceberg.base.SinkPropertyManager
import services.iceberg.{IcebergSinkEntityManager, IcebergSinkTablePropertyManager}
import services.streaming.base.TimestampOnlyWatermark
import tests.shared.{IcebergCatalogInfo, IcebergUtil, TestDynamicSinkSettings, TestPluginStreamContextImpl}

import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.types.Types
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO, ZLayer}

import java.time.OffsetDateTime
import java.util.UUID

object WatermarkKeyMigrationTests extends ZIOSpecDefault:

  private val tableSchema = new IcebergSchema(
    Types.NestedField.optional(1, "id", Types.StringType.get())
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

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("WatermarkKeyMigration")(
    test("migrates legacy watermark property to arcane.core.watermark on provisioned table") {
      val tableName       = s"wm_migration_${UUID.randomUUID().toString.replace("-", "")}"
      val legacyWatermark = TimestampOnlyWatermark(OffsetDateTime.now())
      val migration       = new WatermarkKeyMigration()

      withProvisionedTable(tableName) { (_, propertyManager, context) =>
        for
          _ <- propertyManager.setProperty(tableName, "arcane-watermark", legacyWatermark.toJson)
          _ <- migration.execute(propertyManager, context)
          newWatermark <- propertyManager.getRequiredProperty(
            tableName,
            MetadataKeys.getFrameworkKey(MetadataKeys.watermarkKey)
          )
          legacyWatermarkProperty <- propertyManager.getProperty(
            tableName,
            "arcane-watermark"
          )
        yield assertTrue(newWatermark == legacyWatermark.toJson, legacyWatermarkProperty.isEmpty)
      }
    },
    test("does not set property when legacy watermark is not present on provisioned table") {
      val tableName = s"wm_migration_none_${UUID.randomUUID().toString.replace("-", "")}"
      val migration = new WatermarkKeyMigration()

      withProvisionedTable(tableName) { (_, propertyManager, context) =>
        for
          _ <- migration.execute(propertyManager, context)
          newWatermark <- propertyManager.getProperty(
            tableName,
            MetadataKeys.getFrameworkKey(MetadataKeys.watermarkKey)
          )
        yield assertTrue(newWatermark.isEmpty)
      }
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
