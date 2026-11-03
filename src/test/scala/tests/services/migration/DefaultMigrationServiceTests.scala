package com.sneaksanddata.arcane.framework
package tests.services.migration

import migrations.base.{CurrentMigration, Migration}
import models.MetadataKeys
import models.app.PluginStreamContext
import models.ddl.CreateTableRequest
import models.settings.sink.SinkSettings
import services.base.{MergeServiceClient, StreamingSource}
import services.iceberg.base.SinkPropertyManager
import services.iceberg.{IcebergSinkEntityManager, IcebergSinkTablePropertyManager}
import services.merging.JdbcMergeServiceClient
import services.metrics.DeclaredMetrics
import services.migration.DefaultMigrationService
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
import zio.{Ref, Scope, Task, ZIO}

import java.util.UUID

object DefaultMigrationServiceTests extends ZIOSpecDefault:

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

  private val appliedMigrationsProperty = MetadataKeys.getFrameworkKey(MetadataKeys.appliedMigrationsKey)

  /** Migration that records every invocation so repeated runs of expensive work can be detected.
    */
  private class CountingMigration(val uniqueId: Long, counter: Ref[Int], fail: Boolean = false) extends Migration:
    val shortName: String   = s"counting_$uniqueId"
    val description: String = s"Counting migration $uniqueId"

    def execute(
        sinkPropertyManager: SinkPropertyManager,
        context: PluginStreamContext,
        mergeClient: MergeServiceClient,
        streamingSource: StreamingSource
    ): ZIO[Any, Throwable, Unit] =
      counter.update(_ + 1) *> ZIO.when(fail)(ZIO.fail(new RuntimeException("migration failed"))).unit

  private def withProvisionedTable[R, A](tableName: String)(
      testLogic: (IcebergSinkTablePropertyManager, PluginStreamContext) => ZIO[R, Throwable, A]
  ): ZIO[R & Scope, Throwable, A] =
    val icebergUtil = IcebergUtil(IcebergCatalogInfo.defaultSinkSettings)
    for
      entityManager   <- icebergUtil.getSinkEntityManager
      propertyManager <- icebergUtil.getSinkTablePropertyManager
      context = new TestPluginStreamContextImpl:
        override val sink: SinkSettings = new TestDynamicSinkSettings(s"iceberg.test.$tableName")
      result <- ZIO.acquireReleaseWith(
        entityManager.createTable(CreateTableRequest(tableName, tableSchema, true))
      )(_ => entityManager.delete(tableName).orDie) { _ =>
        testLogic(propertyManager, context)
      }
    yield result

  private def createService(
      propertyManager: IcebergSinkTablePropertyManager,
      context: PluginStreamContext
  ): DefaultMigrationService =
    new DefaultMigrationService(propertyManager, context, testMergeServiceClient, TestStreamingSource)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("DefaultMigrationServiceTests")(
    test("records applied migration identifier on the table after a successful execution") {
      val tableName = s"mig_svc_${UUID.randomUUID().toString.replace("-", "")}"

      withProvisionedTable(tableName) { (propertyManager, context) =>
        for
          counter <- Ref.make(0)
          service   = createService(propertyManager, context)
          migration = new CountingMigration(111L, counter)
          _          <- service.execute(migration)
          applied    <- propertyManager.getRequiredProperty(tableName, appliedMigrationsProperty)
          executions <- counter.get
        yield assertTrue(
          applied == "111",
          executions == 1
        )
      }
    },
    test("accumulates multiple applied migration identifiers in a single property") {
      val tableName = s"mig_svc_${UUID.randomUUID().toString.replace("-", "")}"

      withProvisionedTable(tableName) { (propertyManager, context) =>
        for
          counter <- Ref.make(0)
          service = createService(propertyManager, context)
          _       <- service.execute(new CountingMigration(222L, counter))
          _       <- service.execute(new CountingMigration(111L, counter))
          applied <- propertyManager.getRequiredProperty(tableName, appliedMigrationsProperty)
        yield assertTrue(applied == "111,222")
      }
    },
    test("does not re-run an expensive migration when a later migration in the same run failed") {
      val tableName = s"mig_svc_${UUID.randomUUID().toString.replace("-", "")}"

      withProvisionedTable(tableName) { (propertyManager, context) =>
        for
          expensiveCounter <- Ref.make(0)
          failingCounter   <- Ref.make(0)
          service   = createService(propertyManager, context)
          expensive = new CountingMigration(500L, expensiveCounter)
          failing   = new CountingMigration(600L, failingCounter, fail = true)
          firstRunPending <- service.getPendingMigrations(Seq(expensive, failing))
          _               <- service.execute(expensive)
          // the second migration of the run fails, so the run never reaches commitMigration
          firstRunFailure   <- service.execute(failing).exit
          secondRunPending  <- service.getPendingMigrations(Seq(expensive, failing))
          _                 <- service.execute(failing).exit
          expensiveRunCount <- expensiveCounter.get
          committed <- propertyManager.getProperty(
            tableName,
            MetadataKeys.getFrameworkKey(MetadataKeys.migrationKey)
          )
        yield assertTrue(
          firstRunPending.map(_.uniqueId) == Seq(500L, 600L),
          firstRunFailure.isFailure,
          // the expensive migration is skipped on the retry, only the failing one is attempted again
          secondRunPending.map(_.uniqueId) == Seq(600L),
          expensiveRunCount == 1,
          committed.isEmpty
        )
      }
    },
    test("removes the applied migrations property once the whole run is committed") {
      val tableName = s"mig_svc_${UUID.randomUUID().toString.replace("-", "")}"

      withProvisionedTable(tableName) { (propertyManager, context) =>
        for
          counter <- Ref.make(0)
          service = createService(propertyManager, context)
          _             <- service.execute(new CountingMigration(777L, counter))
          appliedBefore <- propertyManager.getRequiredProperty(tableName, appliedMigrationsProperty)
          _             <- service.commitMigration
          appliedAfter  <- propertyManager.getProperty(tableName, appliedMigrationsProperty)
          committed <- propertyManager.getRequiredProperty(
            tableName,
            MetadataKeys.getFrameworkKey(MetadataKeys.migrationKey)
          )
          pendingAfter <- service.getPendingMigrations(Seq(new CountingMigration(777L, counter)))
        yield assertTrue(
          appliedBefore == "777",
          appliedAfter.isEmpty,
          committed == CurrentMigration.toString,
          // with the checkpoint cleared, a fresh run would consider the migration pending again
          pendingAfter.map(_.uniqueId) == Seq(777L)
        )
      }
    },
    test("keeps all migrations pending when no checkpoint property is present") {
      val tableName = s"mig_svc_${UUID.randomUUID().toString.replace("-", "")}"

      withProvisionedTable(tableName) { (propertyManager, context) =>
        for
          counter <- Ref.make(0)
          service = createService(propertyManager, context)
          pending <- service.getPendingMigrations(
            Seq(new CountingMigration(1L, counter), new CountingMigration(2L, counter))
          )
          applied <- propertyManager.getProperty(tableName, appliedMigrationsProperty)
        yield assertTrue(
          pending.map(_.uniqueId) == Seq(1L, 2L),
          applied.isEmpty
        )
      }
    }
  ) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock
