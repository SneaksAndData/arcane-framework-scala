package com.sneaksanddata.arcane.framework
package tests.services.mssql.migrations

import models.app.PluginStreamContext
import models.ddl.CreateTableRequest
import models.schemas.{MergeKeyField, VersionField}
import models.settings.mssql.MsSqlServerDatabaseSourceSettings
import models.settings.sink.SinkSettings
import services.iceberg.base.SinkPropertyManager
import services.iceberg.{IcebergSinkEntityManager, IcebergSinkTablePropertyManager}
import services.merging.JdbcMergeServiceClient
import services.metrics.DeclaredMetrics
import services.mssql.base.{ColumnSummary, MsSqlStreamingSource}
import services.mssql.migrations.SqlServerArcaneVersionMigration
import services.naming.DefaultNameGenerator
import tests.shared.{
  IcebergCatalogInfo,
  IcebergUtil,
  TestDynamicSinkSettings,
  TestJdbcMergeServiceClientSettings,
  TestPluginStreamContextImpl,
  TestTrinoConnection
}

import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.types.Types
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, Task, ZIO}

import java.sql.Connection
import java.util.UUID

object SqlServerArcaneVersionMigrationTests extends ZIOSpecDefault:

  private val mergeClient = new JdbcMergeServiceClient(
    TestJdbcMergeServiceClientSettings,
    "iceberg",
    "test",
    DeclaredMetrics(),
    false
  )

  private def createMsSqlSource(columnSummaries: List[ColumnSummary]): MsSqlStreamingSource =
    val connectionSettings = new MsSqlServerDatabaseSourceSettings:
      override val connectionUrl: String                          = "jdbc:sqlserver://localhost:1433"
      override val schemaName: String                             = "dbo"
      override val tableName: String                              = "test_table"
      override val fetchSize: Option[Int]                         = None
      override val extraConnectionParameters: Map[String, String] = Map.empty
      override val shardSizeMegabytes: Option[Int]                = None
      override val backfillShardSchemaName: String                = "dbo"

    val nameGenerator = DefaultNameGenerator(
      sinkSettings = TestDynamicSinkSettings("target_table"),
      backfillId = "backfill",
      streamId = "stream"
    )

    new MsSqlStreamingSource(
      connectionSettings = connectionSettings,
      nameGenerator = nameGenerator,
      modifications = Seq.empty,
      connection = null,
      columnSummaries = columnSummaries,
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

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("SqlServerArcaneVersionMigrationTests")(
    test(
      "adds ARCANE_VERSION, copies ChangeTrackingVersion into it, then drops legacy ChangeTrackingVersion column"
    ) {
      val tableName = s"mssql_ver_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, MergeKeyField.name, Types.StringType.get()),
        Types.NestedField.optional(2, "ChangeTrackingVersion", Types.LongType.get()),
        Types.NestedField.optional(3, "val", Types.StringType.get())
      )
      val migration = new SqlServerArcaneVersionMigration()

      withProvisionedTable(tableName, schema) { (_, propertyManager, context, trinoCon) =>
        val source = createMsSqlSource(List(("val", false)))
        for
          _             <- insertRow(trinoCon, tableName, "values ('mk_1', 100, 'hello')")
          columnsBefore <- propertyManager.getTableSchema(tableName).map(_.columns())
          _             <- migration.execute(propertyManager, context, mergeClient, source)
          updatedVersion <- TestTrinoConnection.getFieldValueInTarget(
            trinoCon,
            s"iceberg.test.$tableName",
            VersionField.name,
            "val",
            "hello"
          )
          columnsAfter <- propertyManager.getTableSchema(tableName).map(_.columns())
        yield assertTrue(
          !columnsBefore.stream().anyMatch(_.name().equalsIgnoreCase(VersionField.name)),
          updatedVersion == "100",
          !columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase("ChangeTrackingVersion")),
          columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase(VersionField.name)),
          columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase(MergeKeyField.name)),
          columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase("val"))
        )
      }
    },
    test("is a no-op when legacy ChangeTrackingVersion column is already absent") {
      val tableName = s"mssql_ver_noop_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, MergeKeyField.name, Types.StringType.get()),
        Types.NestedField.optional(2, "val", Types.StringType.get())
      )
      val migration = new SqlServerArcaneVersionMigration()

      withProvisionedTable(tableName, schema) { (_, propertyManager, context, trinoCon) =>
        val source = createMsSqlSource(List(("val", false)))
        for
          _       <- insertRow(trinoCon, tableName, "values ('mk_1', 'hello')")
          _       <- migration.execute(propertyManager, context, mergeClient, source)
          columns <- propertyManager.getTableSchema(tableName).map(_.columns())
        yield assertTrue(
          columns.stream().anyMatch(_.name().equalsIgnoreCase(MergeKeyField.name)),
          columns.stream().anyMatch(_.name().equalsIgnoreCase("val"))
        )
      }
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
