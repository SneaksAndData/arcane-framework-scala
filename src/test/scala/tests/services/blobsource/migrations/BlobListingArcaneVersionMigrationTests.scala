package com.sneaksanddata.arcane.framework
package tests.services.blobsource.migrations

import models.app.PluginStreamContext
import models.batches.BlobBatchCommons
import models.ddl.CreateTableRequest
import models.schemas.{MergeKeyField, VersionField}
import models.settings.sink.SinkSettings
import services.blobsource.migrations.BlobListingArcaneVersionMigration
import services.iceberg.base.SinkPropertyManager
import services.iceberg.{IcebergSinkEntityManager, IcebergSinkTablePropertyManager}
import services.merging.JdbcMergeServiceClient
import services.metrics.DeclaredMetrics
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
import java.util.UUID

object BlobListingArcaneVersionMigrationTests extends ZIOSpecDefault:

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

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("BlobListingArcaneVersionMigrationTests")(
    test(
      "adds ARCANE_VERSION, copies createdon into it, then drops legacy createdon column"
    ) {
      val tableName = s"blob_ver_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, MergeKeyField.name, Types.StringType.get()),
        Types.NestedField.optional(2, BlobBatchCommons.versionField.name, Types.LongType.get()),
        Types.NestedField.optional(3, "name", Types.StringType.get())
      )
      val migration = new BlobListingArcaneVersionMigration()

      withProvisionedTable(tableName, schema) { (_, propertyManager, context, trinoCon) =>
        for
          _             <- insertRow(trinoCon, tableName, "values ('legacy_key_1', 123456789, 'file1')")
          columnsBefore <- propertyManager.getTableSchema(tableName).map(_.columns())
          _             <- migration.execute(propertyManager, context, mergeClient, TestStreamingSource)
          updatedVersion <- TestTrinoConnection.getFieldValueInTarget(
            trinoCon,
            s"iceberg.test.$tableName",
            VersionField.name,
            "name",
            "file1"
          )
          columnsAfter <- propertyManager.getTableSchema(tableName).map(_.columns())
        yield assertTrue(
          !columnsBefore.stream().anyMatch(_.name().equalsIgnoreCase(VersionField.name)),
          updatedVersion == "123456789",
          !columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase(BlobBatchCommons.versionField.name)),
          columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase(VersionField.name)),
          columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase(MergeKeyField.name)),
          columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase("name"))
        )
      }
    },
    test("is a no-op when legacy createdon column is already absent") {
      val tableName = s"blob_ver_noop_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, MergeKeyField.name, Types.StringType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get())
      )
      val migration = new BlobListingArcaneVersionMigration()

      withProvisionedTable(tableName, schema) { (_, propertyManager, context, trinoCon) =>
        for
          _       <- insertRow(trinoCon, tableName, "values ('legacy_key_1', 'file1')")
          _       <- migration.execute(propertyManager, context, mergeClient, TestStreamingSource)
          columns <- propertyManager.getTableSchema(tableName).map(_.columns())
        yield assertTrue(
          columns.stream().anyMatch(_.name().equalsIgnoreCase(MergeKeyField.name)),
          columns.stream().anyMatch(_.name().equalsIgnoreCase("name"))
        )
      }
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
