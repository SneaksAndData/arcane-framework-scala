package com.sneaksanddata.arcane.framework
package tests.services.pullstream.migrations

import models.app.PluginStreamContext
import models.ddl.CreateTableRequest
import models.schemas.VersionField
import models.settings.sink.SinkSettings
import services.iceberg.base.SinkPropertyManager
import services.iceberg.{IcebergSinkEntityManager, IcebergSinkTablePropertyManager}
import services.merging.JdbcMergeServiceClient
import services.metrics.DeclaredMetrics
import services.pullstream.migrations.PullStreamVersionFieldMigration
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
import zio.{Scope, Task, ZIO}

import java.sql.Connection
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.UUID

object PullStreamVersionFieldMigrationTests extends ZIOSpecDefault:

  private val legacyVersionFieldName = "timestamputc"

  private val mergeClient = new JdbcMergeServiceClient(
    TestJdbcMergeServiceClientSettings,
    "iceberg",
    "test",
    DeclaredMetrics(),
    false
  )

  private def withProvisionedTable[R, A](tableName: String, schema: IcebergSchema)(
      testLogic: (IcebergSinkEntityManager, IcebergSinkTablePropertyManager, PluginStreamContext, Connection) => ZIO[
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
      trinoCon <- ZIO.acquireRelease(TestTrinoConnection.newTrinoConnection)(c => ZIO.attemptBlocking(c.close()).orDie)
      result <- ZIO.acquireReleaseWith(
        entityManager.createTable(CreateTableRequest(tableName, schema, true))
      )(_ => entityManager.delete(tableName).orDie) { _ =>
        testLogic(entityManager, propertyManager, context, trinoCon)
      }
    yield result

  private def insertRow(con: Connection, tableName: String, valuesSql: String): Task[Unit] =
    ZIO.scoped {
      for
        st <- ZIO.fromAutoCloseable(ZIO.attemptBlocking(con.createStatement()))
        _  <- ZIO.attemptBlocking(st.execute(s"insert into iceberg.test.$tableName $valuesSql"))
      yield ()
    }

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("PullStreamVersionFieldMigrationTests")(
    test("moves the legacy watermark column into ARCANE_VERSION and drops it") {
      val tableName = s"pull_vf_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, legacyVersionFieldName, Types.StringType.get()),
        Types.NestedField.optional(2, "userId", Types.StringType.get()),
        Types.NestedField.optional(3, VersionField.name, Types.LongType.get())
      )
      val migration      = new PullStreamVersionFieldMigration(legacyVersionFieldName)
      val timestamp      = OffsetDateTime.ofInstant(Instant.parse("2026-08-04T12:34:56.000Z"), ZoneOffset.UTC)
      val expectedMillis = timestamp.toInstant.toEpochMilli

      withProvisionedTable(tableName, schema) { (_, propertyManager, context, trinoCon) =>
        for
          _ <- insertRow(
            trinoCon,
            tableName,
            s"values ('${timestamp.toString}', 'user-1', NULL)"
          )
          _ <- migration.execute(propertyManager, context, mergeClient, TestStreamingSource)
          updatedVersion <- TestTrinoConnection.getFieldValueInTarget(
            trinoCon,
            s"iceberg.test.$tableName",
            VersionField.name,
            "userId",
            "user-1"
          )
          columns <- propertyManager.getTableSchema(tableName).map(_.columns())
        yield assertTrue(
          updatedVersion == expectedMillis.toString,
          !columns.stream().anyMatch(_.name().equalsIgnoreCase(legacyVersionFieldName))
        )
      }
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
