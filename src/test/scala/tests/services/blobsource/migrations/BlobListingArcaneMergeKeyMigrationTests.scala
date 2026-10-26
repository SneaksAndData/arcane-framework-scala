package com.sneaksanddata.arcane.framework
package tests.services.blobsource.migrations

import models.app.PluginStreamContext
import models.ddl.CreateTableRequest
import models.schemas.MergeKeyField
import models.settings.sink.SinkSettings
import services.blobsource.migrations.BlobListingArcaneMergeKeyMigration
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
import utils.HashUtils

import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.types.Types
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, Task, ZIO}

import java.sql.Connection
import java.util.UUID

object BlobListingArcaneMergeKeyMigrationTests extends ZIOSpecDefault:

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

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("BlobListingArcaneMergeKeyMigrationTests")(
    test("migrates single primary key ARCANE_MERGE_KEY on provisioned table") {
      val tableName = s"blob_mk_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, MergeKeyField.name, Types.StringType.get()),
        Types.NestedField.optional(2, "file_id", Types.StringType.get()),
        Types.NestedField.optional(3, "content", Types.StringType.get())
      )
      val migration = new BlobListingArcaneMergeKeyMigration(Seq("file_id"))

      withProvisionedTable(tableName, schema) { (_, propertyManager, context, trinoCon) =>
        for
          _ <- insertRow(trinoCon, tableName, "values ('legacy_key_1', 'f-1', 'data1')")
          _ <- migration.execute(propertyManager, context, mergeClient, TestStreamingSource)
          updatedKey <- TestTrinoConnection.getFieldValueInTarget(
            trinoCon,
            s"iceberg.test.$tableName",
            MergeKeyField.name,
            "file_id",
            "f-1"
          )
        yield assertTrue(updatedKey == HashUtils.murmur3("f-1"))
      }
    },
    test("migrates composite primary keys ARCANE_MERGE_KEY on provisioned table") {
      val tableName = s"blob_mk_comp_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, MergeKeyField.name, Types.StringType.get()),
        Types.NestedField.optional(2, "batch_id", Types.StringType.get()),
        Types.NestedField.optional(3, "item_id", Types.StringType.get()),
        Types.NestedField.optional(4, "value", Types.StringType.get())
      )
      val migration = new BlobListingArcaneMergeKeyMigration(Seq("batch_id", "item_id"))

      withProvisionedTable(tableName, schema) { (_, propertyManager, context, trinoCon) =>
        for
          _ <- insertRow(trinoCon, tableName, "values ('legacy_key_comp', 'b1', 'i2', 'val')")
          _ <- migration.execute(propertyManager, context, mergeClient, TestStreamingSource)
          updatedKey <- TestTrinoConnection.getFieldValueInTarget(
            trinoCon,
            s"iceberg.test.$tableName",
            MergeKeyField.name,
            "batch_id",
            "b1"
          )
        yield assertTrue(updatedKey == HashUtils.murmur3("b1#i2"))
      }
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
