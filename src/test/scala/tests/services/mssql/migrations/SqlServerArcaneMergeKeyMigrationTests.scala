package com.sneaksanddata.arcane.framework
package tests.services.mssql.migrations

import exceptions.FatalStreamFailException
import models.app.PluginStreamContext
import models.ddl.CreateTableRequest
import models.schemas.MergeKeyField
import models.settings.mssql.MsSqlServerDatabaseSourceSettings
import models.settings.sink.SinkSettings
import services.iceberg.base.SinkPropertyManager
import services.iceberg.{IcebergSinkEntityManager, IcebergSinkTablePropertyManager}
import services.merging.JdbcMergeServiceClient
import services.metrics.DeclaredMetrics
import services.mssql.base.{ColumnSummary, MsSqlStreamingSource}
import services.mssql.migrations.SqlServerArcaneMergeKeyMigration
import services.naming.DefaultNameGenerator
import tests.shared.{
  IcebergCatalogInfo,
  IcebergUtil,
  TestDynamicSinkSettings,
  TestJdbcMergeServiceClientSettings,
  TestPluginStreamContextImpl,
  TestTrinoConnection
}
import utils.HashUtils

import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.types.Types
import org.scalatestplus.easymock.EasyMockSugar.mock
import zio.test.*
import zio.test.Assertion.fails
import zio.test.TestAspect.timeout
import zio.{Scope, Task, ZIO}

import java.sql.Connection
import java.util.UUID

object SqlServerArcaneMergeKeyMigrationTests extends ZIOSpecDefault:

  private val mergeClient = new JdbcMergeServiceClient(
    TestJdbcMergeServiceClientSettings,
    "iceberg",
    "test",
    DeclaredMetrics(),
    false
  )

  private def createMsSqlSource(summaries: List[ColumnSummary]): MsSqlStreamingSource =
    val connectionSettings = new MsSqlServerDatabaseSourceSettings:
      override val connectionUrl: String                          = "jdbc:sqlserver://localhost:1433"
      override val schemaName: String                             = "dbo"
      override val tableName: String                              = "test_table"
      override val fetchSize: Option[Int]                         = None
      override val extraConnectionParameters: Map[String, String] = Map.empty
      override val shardSizeMegabytes: Option[Int]                = None
      override val backfillShardSchemaName: String                = "dbo"

    val nameGenerator = new DefaultNameGenerator(
      TestDynamicSinkSettings("iceberg.test.test_table"),
      "backfill-1",
      "stream-1"
    )

    new MsSqlStreamingSource(
      connectionSettings = connectionSettings,
      nameGenerator = nameGenerator,
      modifications = Seq.empty,
      connection = mock[Connection],
      columnSummaries = summaries,
      sqlSchema = Seq.empty
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

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("SqlServerArcaneMergeKeyMigrationTests")(
    test("migrates single primary key ARCANE_MERGE_KEY on provisioned table") {
      val tableName = s"merge_key_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, MergeKeyField.name, Types.StringType.get()),
        Types.NestedField.optional(2, "id", Types.StringType.get()),
        Types.NestedField.optional(3, "val", Types.StringType.get())
      )
      val migration = new SqlServerArcaneMergeKeyMigration()

      withProvisionedTable(tableName, schema) { (_, propertyManager, context, trinoCon) =>
        val source = createMsSqlSource(List(("id", true), ("val", false)))
        for
          _ <- insertRow(trinoCon, tableName, "values ('legacy_key_1', 'pk_1', 'hello')")
          _ <- migration.execute(propertyManager, context, mergeClient, source)
          updatedKey <- TestTrinoConnection.getFieldValueInTarget(
            trinoCon,
            s"iceberg.test.$tableName",
            MergeKeyField.name,
            "id",
            "pk_1"
          )
        yield assertTrue(updatedKey == HashUtils.murmur3("pk_1"))
      }
    },
    test("migrates composite primary keys ARCANE_MERGE_KEY on provisioned table") {
      val tableName = s"merge_key_comp_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, MergeKeyField.name, Types.StringType.get()),
        Types.NestedField.optional(2, "tenant_id", Types.StringType.get()),
        Types.NestedField.optional(3, "user_id", Types.StringType.get()),
        Types.NestedField.optional(4, "data", Types.StringType.get())
      )
      val migration = new SqlServerArcaneMergeKeyMigration()

      withProvisionedTable(tableName, schema) { (_, propertyManager, context, trinoCon) =>
        val source = createMsSqlSource(List(("tenant_id", true), ("user_id", true), ("data", false)))
        for
          _ <- insertRow(trinoCon, tableName, "values ('legacy_composite_key', 'tenant_A', 'user_42', 'some_data')")
          _ <- migration.execute(propertyManager, context, mergeClient, source)
          updatedKey <- TestTrinoConnection.getFieldValueInTarget(
            trinoCon,
            s"iceberg.test.$tableName",
            MergeKeyField.name,
            "tenant_id",
            "tenant_A"
          )
        yield assertTrue(updatedKey == HashUtils.murmur3("tenant_A#user_42"))
      }
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
