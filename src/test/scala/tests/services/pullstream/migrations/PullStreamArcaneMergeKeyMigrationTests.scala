package com.sneaksanddata.arcane.framework
package tests.services.pullstream.migrations

import migrations.base.MergeKeyColumnRebuild
import models.app.PluginStreamContext
import models.ddl.CreateTableRequest
import models.schemas.MergeKeyField
import models.settings.sink.SinkSettings
import services.iceberg.base.SinkPropertyManager
import services.iceberg.{IcebergSinkEntityManager, IcebergSinkTablePropertyManager}
import services.merging.JdbcMergeServiceClient
import services.metrics.DeclaredMetrics
import services.pullstream.migrations.PullStreamArcaneMergeKeyMigration
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
import scala.jdk.CollectionConverters.*

object PullStreamArcaneMergeKeyMigrationTests extends ZIOSpecDefault:

  /** Name of the merge key column in tables created before 2.4 */
  private val legacyMergeKeyName = "ARCANE_MERGE_KEY"

  private val tmpMergeKeyName = s"${MergeKeyField.name}_tmp"

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

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("PullStreamArcaneMergeKeyMigrationTests")(
    test("migrates ARCANE_MERGE_KEY on provisioned table using existing raw key") {
      val tableName = s"pull_mk_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, legacyMergeKeyName, Types.StringType.get()),
        Types.NestedField.optional(2, "userId", Types.StringType.get()),
        Types.NestedField.optional(3, "level", Types.StringType.get())
      )
      val migration = new PullStreamArcaneMergeKeyMigration()

      withProvisionedTable(tableName, schema) { (_, propertyManager, context, trinoCon) =>
        for
          _ <- insertRow(trinoCon, tableName, "values ('raw_push_id_1', 'user-1', 'admin')")
          _ <- insertRow(trinoCon, tableName, "values ('raw_push_id_2', 'user-2', 'user')")
          _ <- migration.execute(propertyManager, context, mergeClient, TestStreamingSource)
          updatedKey1 <- TestTrinoConnection.getFieldValueInTarget(
            trinoCon,
            s"iceberg.test.$tableName",
            MergeKeyField.name,
            "userId",
            "user-1"
          )
          updatedKey2 <- TestTrinoConnection.getFieldValueInTarget(
            trinoCon,
            s"iceberg.test.$tableName",
            MergeKeyField.name,
            "userId",
            "user-2"
          )
          columns  <- propertyManager.getTableSchema(tableName).map(_.columns().asScala.map(_.name()).toList)
          progress <- propertyManager.getProperty(tableName, MergeKeyColumnRebuild.progressProperty)
        yield assertTrue(
          updatedKey1 == HashUtils.murmur3("raw_push_id_1"),
          progress.isEmpty,
          updatedKey2 == HashUtils.murmur3("raw_push_id_2"),
          columns.contains(MergeKeyField.name),
          !columns.contains(legacyMergeKeyName),
          !columns.exists(_.endsWith("_tmp"))
        )
      }
    },
    test("resumes after the temporary column was computed") {
      val tableName = s"pull_mk_step1_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, legacyMergeKeyName, Types.StringType.get()),
        Types.NestedField.optional(2, tmpMergeKeyName, Types.StringType.get()),
        Types.NestedField.optional(3, "userId", Types.StringType.get())
      )

      withProvisionedTable(tableName, schema) { (_, propertyManager, context, trinoCon) =>
        for
          _ <- insertRow(trinoCon, tableName, "values ('raw_push_id_1', 'computed_key_1', 'user-1')")
          _ <- propertyManager.setProperty(tableName, MergeKeyColumnRebuild.progressProperty, "1")
          _ <- new PullStreamArcaneMergeKeyMigration()
            .execute(propertyManager, context, mergeClient, TestStreamingSource)
          key <- TestTrinoConnection.getFieldValueInTarget(
            trinoCon,
            s"iceberg.test.$tableName",
            MergeKeyField.name,
            "userId",
            "user-1"
          )
          columns  <- propertyManager.getTableSchema(tableName).map(_.columns().asScala.map(_.name()).toList)
          progress <- propertyManager.getProperty(tableName, MergeKeyColumnRebuild.progressProperty)
        yield assertTrue(
          key == "computed_key_1",
          columns.contains(MergeKeyField.name),
          !columns.contains(legacyMergeKeyName),
          !columns.contains(tmpMergeKeyName),
          progress.isEmpty
        )
      }
    },
    test("resumes after the legacy column was dropped") {
      val tableName = s"pull_mk_step2_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, tmpMergeKeyName, Types.StringType.get()),
        Types.NestedField.optional(2, "userId", Types.StringType.get())
      )

      withProvisionedTable(tableName, schema) { (_, propertyManager, context, trinoCon) =>
        for
          _ <- insertRow(trinoCon, tableName, "values ('computed_key_2', 'user-1')")
          _ <- propertyManager.setProperty(tableName, MergeKeyColumnRebuild.progressProperty, "2")
          _ <- new PullStreamArcaneMergeKeyMigration()
            .execute(propertyManager, context, mergeClient, TestStreamingSource)
          key <- TestTrinoConnection.getFieldValueInTarget(
            trinoCon,
            s"iceberg.test.$tableName",
            MergeKeyField.name,
            "userId",
            "user-1"
          )
          columns  <- propertyManager.getTableSchema(tableName).map(_.columns().asScala.map(_.name()).toList)
          progress <- propertyManager.getProperty(tableName, MergeKeyColumnRebuild.progressProperty)
        yield assertTrue(
          key == "computed_key_2",
          columns.contains(MergeKeyField.name),
          !columns.contains(legacyMergeKeyName),
          !columns.contains(tmpMergeKeyName),
          progress.isEmpty
        )
      }
    },
    test("resumes when the new column was added but not filled") {
      val tableName = s"pull_mk_step2b_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, tmpMergeKeyName, Types.StringType.get()),
        Types.NestedField.optional(2, MergeKeyField.name, Types.StringType.get()),
        Types.NestedField.optional(3, "userId", Types.StringType.get())
      )

      withProvisionedTable(tableName, schema) { (_, propertyManager, context, trinoCon) =>
        for
          _ <- insertRow(trinoCon, tableName, "values ('computed_key_3', NULL, 'user-1')")
          _ <- propertyManager.setProperty(tableName, MergeKeyColumnRebuild.progressProperty, "2")
          _ <- new PullStreamArcaneMergeKeyMigration()
            .execute(propertyManager, context, mergeClient, TestStreamingSource)
          key <- TestTrinoConnection.getFieldValueInTarget(
            trinoCon,
            s"iceberg.test.$tableName",
            MergeKeyField.name,
            "userId",
            "user-1"
          )
          columns  <- propertyManager.getTableSchema(tableName).map(_.columns().asScala.map(_.name()).toList)
          progress <- propertyManager.getProperty(tableName, MergeKeyColumnRebuild.progressProperty)
        yield assertTrue(
          key == "computed_key_3",
          columns.contains(MergeKeyField.name),
          !columns.contains(legacyMergeKeyName),
          !columns.contains(tmpMergeKeyName),
          progress.isEmpty
        )
      }
    },
    test("resumes after the values were copied") {
      val tableName = s"pull_mk_step3_${UUID.randomUUID().toString.replace("-", "")}"
      val schema = new IcebergSchema(
        Types.NestedField.optional(1, tmpMergeKeyName, Types.StringType.get()),
        Types.NestedField.optional(2, MergeKeyField.name, Types.StringType.get()),
        Types.NestedField.optional(3, "userId", Types.StringType.get())
      )

      withProvisionedTable(tableName, schema) { (_, propertyManager, context, trinoCon) =>
        for
          _ <- insertRow(trinoCon, tableName, "values ('computed_key_4', 'computed_key_4', 'user-1')")
          _ <- propertyManager.setProperty(tableName, MergeKeyColumnRebuild.progressProperty, "3")
          _ <- new PullStreamArcaneMergeKeyMigration()
            .execute(propertyManager, context, mergeClient, TestStreamingSource)
          key <- TestTrinoConnection.getFieldValueInTarget(
            trinoCon,
            s"iceberg.test.$tableName",
            MergeKeyField.name,
            "userId",
            "user-1"
          )
          columns  <- propertyManager.getTableSchema(tableName).map(_.columns().asScala.map(_.name()).toList)
          progress <- propertyManager.getProperty(tableName, MergeKeyColumnRebuild.progressProperty)
        yield assertTrue(
          key == "computed_key_4",
          columns.contains(MergeKeyField.name),
          !columns.contains(legacyMergeKeyName),
          !columns.contains(tmpMergeKeyName),
          progress.isEmpty
        )
      }
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
