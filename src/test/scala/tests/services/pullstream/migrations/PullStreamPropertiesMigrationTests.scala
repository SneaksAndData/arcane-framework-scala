package com.sneaksanddata.arcane.framework
package tests.services.pullstream.migrations

import models.MetadataKeys
import models.app.PluginStreamContext
import models.ddl.CreateTableRequest
import models.settings.sink.SinkSettings
import services.iceberg.base.SinkPropertyManager
import services.iceberg.{IcebergSinkEntityManager, IcebergSinkTablePropertyManager}
import services.merging.JdbcMergeServiceClient
import services.metrics.DeclaredMetrics
import services.pullstream.PullStreamingSource
import services.pullstream.migrations.PullStreamPropertiesMigration
import tests.shared.{
  IcebergCatalogInfo,
  IcebergUtil,
  TestDynamicSinkSettings,
  TestJdbcMergeServiceClientSettings,
  TestPluginStreamContextImpl,
  TestStreamingSource,
  TestTrinoConnection
}

import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.types.Types
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO}

import java.util.UUID

object PullStreamPropertiesMigrationTests extends ZIOSpecDefault:

  private val mergeClient = new JdbcMergeServiceClient(
    TestJdbcMergeServiceClientSettings,
    "iceberg",
    "test",
    DeclaredMetrics(),
    false
  )

  private def withProvisionedTable[R, A](tableName: String, schema: IcebergSchema)(
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
        override val sink: SinkSettings = new TestDynamicSinkSettings(s"iceberg.test.$tableName")
      result <- ZIO.acquireReleaseWith(
        entityManager.createTable(CreateTableRequest(tableName, schema, true))
      )(_ => entityManager.delete(tableName).orDie) { _ =>
        testLogic(entityManager, propertyManager, context)
      }
    yield result

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("PullStreamPropertiesMigrationTests")(
    test("sets default properties and migrates legacy json-pointer-expression when present") {
      val tableName = s"pull_props_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, "userId", Types.StringType.get())
      )
      val migration = new PullStreamPropertiesMigration()

      withProvisionedTable(tableName, schema) { (_, propertyManager, context) =>
        for
          _ <- propertyManager.setProperty(tableName, PullStreamingSource.jsonPointerPropertyName, "/data")
          _ <- migration.execute(propertyManager, context, mergeClient, TestStreamingSource)
          pointerVal <- propertyManager.getRequiredProperty(
            tableName,
            PullStreamingSource.jsonPointerPropertyKey
          )
          pageSizeVal <- propertyManager.getRequiredProperty(
            tableName,
            PullStreamingSource.pageSizePropertyKey
          )
          decodeChunkVal <- propertyManager.getRequiredProperty(
            tableName,
            PullStreamingSource.decodeChunkSizePropertyKey
          )
          legacyPointer <- propertyManager.getProperty(
            tableName,
            PullStreamingSource.jsonPointerPropertyName
          )
        yield assertTrue(
          pointerVal == "/data",
          pageSizeVal == "1000",
          decodeChunkVal == "32",
          legacyPointer.isEmpty
        )
      }
    },
    test("sets default empty pointer when legacy property is absent") {
      val tableName = s"pull_props_none_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, "userId", Types.StringType.get())
      )
      val migration = new PullStreamPropertiesMigration()

      withProvisionedTable(tableName, schema) { (_, propertyManager, context) =>
        for
          _ <- migration.execute(propertyManager, context, mergeClient, TestStreamingSource)
          pointerVal <- propertyManager.getRequiredProperty(
            tableName,
            PullStreamingSource.jsonPointerPropertyKey
          )
          pageSizeVal <- propertyManager.getRequiredProperty(
            tableName,
            PullStreamingSource.pageSizePropertyKey
          )
          decodeChunkVal <- propertyManager.getRequiredProperty(
            tableName,
            PullStreamingSource.decodeChunkSizePropertyKey
          )
        yield assertTrue(
          pointerVal == "",
          pageSizeVal == "1000",
          decodeChunkVal == "32"
        )
      }
    },
    test("does not overwrite already existing properties") {
      val tableName = s"pull_props_exists_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, "userId", Types.StringType.get())
      )
      val migration = new PullStreamPropertiesMigration()

      withProvisionedTable(tableName, schema) { (_, propertyManager, context) =>
        for
          _ <- propertyManager.setProperty(tableName, PullStreamingSource.jsonPointerPropertyKey, "/custom")
          _ <- propertyManager.setProperty(tableName, PullStreamingSource.pageSizePropertyKey, "500")
          _ <- propertyManager.setProperty(tableName, PullStreamingSource.decodeChunkSizePropertyKey, "64")
          _ <- migration.execute(propertyManager, context, mergeClient, TestStreamingSource)
          pointerVal <- propertyManager.getRequiredProperty(
            tableName,
            PullStreamingSource.jsonPointerPropertyKey
          )
          pageSizeVal <- propertyManager.getRequiredProperty(
            tableName,
            PullStreamingSource.pageSizePropertyKey
          )
          decodeChunkVal <- propertyManager.getRequiredProperty(
            tableName,
            PullStreamingSource.decodeChunkSizePropertyKey
          )
        yield assertTrue(
          pointerVal == "/custom",
          pageSizeVal == "500",
          decodeChunkVal == "64"
        )
      }
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
