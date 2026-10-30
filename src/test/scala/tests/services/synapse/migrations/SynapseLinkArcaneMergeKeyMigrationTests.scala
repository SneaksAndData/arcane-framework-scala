package com.sneaksanddata.arcane.framework
package tests.services.synapse.migrations

import exceptions.FatalStreamFailException
import models.app.PluginStreamContext
import models.ddl.CreateTableRequest
import models.schemas.MergeKeyField
import models.settings.sink.SinkSettings
import services.iceberg.base.SinkPropertyManager
import services.iceberg.{IcebergSinkEntityManager, IcebergSinkTablePropertyManager}
import services.merging.JdbcMergeServiceClient
import services.metrics.DeclaredMetrics
import services.storage.models.azure.AdlsStoragePath
import services.storage.services.azure.AzureBlobStorageReader
import services.synapse.base.SynapseLinkStreamingSource
import services.synapse.migrations.SynapseLinkArcaneMergeKeyMigration
import tests.shared.{
  IcebergCatalogInfo,
  IcebergUtil,
  TestAzureStorageInfo,
  TestDynamicSinkSettings,
  TestJdbcMergeServiceClientSettings,
  TestPluginStreamContextImpl,
  TestTrinoConnection
}
import utils.HashUtils

import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.types.Types
import zio.test.*
import zio.test.Assertion.fails
import zio.test.TestAspect.timeout
import zio.{Scope, Task, ZIO}

import java.sql.Connection
import java.util.UUID

object SynapseLinkArcaneMergeKeyMigrationTests extends ZIOSpecDefault:

  private val mergeClient = new JdbcMergeServiceClient(
    TestJdbcMergeServiceClientSettings,
    "iceberg",
    "test",
    DeclaredMetrics(),
    false
  )

  private def createSynapseSource(): SynapseLinkStreamingSource =
    new SynapseLinkStreamingSource(
      location = TestAzureStorageInfo.sourceRoot,
      entityName = "test_entity",
      reader = TestAzureStorageInfo.storageReader,
      modifications = Seq.empty
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

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("SynapseLinkArcaneMergeKeyMigrationTests")(
    test("migrates ARCANE_MERGE_KEY on provisioned table using Id column and drops Id column") {
      val tableName = s"synapse_mk_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, MergeKeyField.name, Types.StringType.get()),
        Types.NestedField.optional(2, "Id", Types.StringType.get()),
        Types.NestedField.optional(3, "name", Types.StringType.get())
      )
      val migration = new SynapseLinkArcaneMergeKeyMigration()

      withProvisionedTable(tableName, schema) { (_, propertyManager, context, trinoCon) =>
        val source = createSynapseSource()
        for
          _ <- insertRow(trinoCon, tableName, "values ('legacy_raw_id_1', 'id-101', 'Alice')")
          _ <- insertRow(trinoCon, tableName, "values ('legacy_raw_id_2', 'id-102', 'Bob')")
          _ <- migration.execute(propertyManager, context, mergeClient, source)
          updatedKey1 <- TestTrinoConnection.getFieldValueInTarget(
            trinoCon,
            s"iceberg.test.$tableName",
            MergeKeyField.name,
            "name",
            "Alice"
          )
          updatedKey2 <- TestTrinoConnection.getFieldValueInTarget(
            trinoCon,
            s"iceberg.test.$tableName",
            MergeKeyField.name,
            "name",
            "Bob"
          )
          columnsAfter <- propertyManager.getTableSchema(tableName).map(_.columns())
        yield assertTrue(
          updatedKey1 == HashUtils.murmur3("id-101"),
          updatedKey2 == HashUtils.murmur3("id-102"),
          !columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase("Id")),
          columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase(MergeKeyField.name)),
          columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase("name"))
        )
      }
    },
    test("is a no-op when legacy Id column is already absent") {
      val tableName = s"synapse_mk_noop_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, MergeKeyField.name, Types.StringType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get())
      )
      val migration = new SynapseLinkArcaneMergeKeyMigration()

      withProvisionedTable(tableName, schema) { (_, propertyManager, context, trinoCon) =>
        val source = createSynapseSource()
        for
          _       <- insertRow(trinoCon, tableName, "values ('existing_key_1', 'Alice')")
          _       <- migration.execute(propertyManager, context, mergeClient, source)
          columns <- propertyManager.getTableSchema(tableName).map(_.columns())
          keyVal <- TestTrinoConnection.getFieldValueInTarget(
            trinoCon,
            s"iceberg.test.$tableName",
            MergeKeyField.name,
            "name",
            "Alice"
          )
        yield assertTrue(
          keyVal == "existing_key_1",
          columns.stream().anyMatch(_.name().equalsIgnoreCase(MergeKeyField.name)),
          columns.stream().anyMatch(_.name().equalsIgnoreCase("name"))
        )
      }
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
