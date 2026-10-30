package com.sneaksanddata.arcane.framework
package tests.services.bootstrap

import models.app.{PluginStreamContext, StreamDocumentation}
import models.ddl.CreateTableRequest
import models.schemas.{ArcaneSchema, MergeKeyField, given_CanAdd_ArcaneSchema}
import models.settings.sink.SinkSettings
import services.base.StreamingSource
import services.bootstrap.DefaultStreamBootstrapper
import services.iceberg.base.SinkPropertyManager
import services.iceberg.{IcebergSinkEntityManager, IcebergSinkTablePropertyManager}
import services.migration.MigrationService
import services.naming.DefaultNameGenerator
import services.streaming.base.TimestampOnlyWatermark
import tests.shared.{IcebergCatalogInfo, IcebergUtil, TestDynamicSinkSettings, TestPluginStreamContextImpl}

import zio.stream.ZStream
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, Task, ZIO}

import java.util.UUID

object DefaultStreamBootstrapperTests extends ZIOSpecDefault:

  private val testSource = new StreamingSource:
    override type SchemaType    = ArcaneSchema
    override type WatermarkType = TimestampOnlyWatermark
    override type ShardMetadata = String

    override def getSchema: Task[SchemaType]                = ZIO.succeed(ArcaneSchema(Seq(MergeKeyField)))
    override def empty: SchemaType                          = ArcaneSchema.empty()
    override def deleteShards(streamId: String): Task[Unit] = ZIO.unit
    override def getShards(
        rangeStart: TimestampOnlyWatermark,
        rangeEnd: TimestampOnlyWatermark
    ): ZStream[Any, Throwable, ShardMetadata] = ZStream.empty

  private def withProvisionedBootstrapper[R, A](
      tableName: String,
      pluginNameVal: String,
      pluginRepoVal: String,
      isMigrating: Boolean = false,
      isCurrentMigration: Boolean = false
  )(
      testLogic: (DefaultStreamBootstrapper, IcebergSinkEntityManager, IcebergSinkTablePropertyManager) => ZIO[
        R,
        Throwable,
        A
      ]
  ): ZIO[R & Scope, Throwable, A] =
    val icebergUtil = IcebergUtil(IcebergCatalogInfo.defaultSinkSettings)
    for
      stagingEntityManager <- icebergUtil.getStagingEntityManager
      sinkEntityManager    <- icebergUtil.getSinkEntityManager
      sinkPropertyManager  <- icebergUtil.getSinkTablePropertyManager
      context = new TestPluginStreamContextImpl:
        override val sink: SinkSettings = new TestDynamicSinkSettings(s"iceberg.test.$tableName")
      nameGenerator = DefaultNameGenerator(
        sinkSettings = context.sink,
        backfillId = "bf",
        streamId = "stream"
      )
      migrationService = new MigrationService:
        override def getSinkMigration =
          if isCurrentMigration then ZIO.succeed(Some(migrations.base.CurrentMigration)) else ZIO.succeed(None)
        override def isCurrent(identifier: migrations.base.SinkMigrationIdentifier) = isCurrentMigration
        override def commitMigration                                                = ZIO.unit
        override def diff(
            fromId: Option[migrations.base.SinkMigrationIdentifier],
            toId: migrations.base.SinkMigrationIdentifier
        ) = Seq.empty
        override def execute(migration: migrations.base.Migration) = ZIO.unit
      bootstrapper = new DefaultStreamBootstrapper(
        stagingEntityManager = stagingEntityManager,
        sinkEntityManager = sinkEntityManager,
        sinkPropertyManager = sinkPropertyManager,
        migrationService = migrationService,
        streamingSource = testSource,
        nameGenerator = nameGenerator,
        stagingSettings = context.staging,
        sinkSettings = context.sink,
        isOverwriting = false,
        isMigrating = isMigrating,
        documentation = StreamDocumentation(pluginNameVal, pluginRepoVal)
      )
      result <- ZIO.acquireReleaseWith(ZIO.unit)(_ => sinkEntityManager.delete(tableName).orDie) { _ =>
        testLogic(bootstrapper, sinkEntityManager, sinkPropertyManager)
      }
    yield result

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("DefaultStreamBootstrapperTests")(
    test("createTargetTable sets comment property with plugin name and github repo link") {
      val tableName       = s"bootstrapper_target_${UUID.randomUUID().toString.replace("-", "")}"
      val plugin          = "mssql-plugin"
      val repo            = "https://github.com/SneaksAndData/arcane-stream-mssql"
      val expectedComment = s"Table managed by $plugin $repo"

      withProvisionedBootstrapper(tableName, plugin, repo) { (bootstrapper, _, propertyManager) =>
        for
          _          <- bootstrapper.createTargetTable
          commentOpt <- propertyManager.getProperty(tableName, "comment")
        yield assertTrue(commentOpt.contains(expectedComment))
      }
    },
    test("checkMigrations succeeds when isMigrating is true even if migrations are not current") {
      val tableName = s"bootstrapper_mig_${UUID.randomUUID().toString.replace("-", "")}"
      withProvisionedBootstrapper(tableName, "p", "r", isMigrating = true) { (bootstrapper, _, _) =>
        for res <- bootstrapper.checkMigrations.exit
        yield assertTrue(res.isSuccess)
      }
    },
    test("checkMigrations fails when isMigrating is false and migrations are not current") {
      val tableName = s"bootstrapper_fail_${UUID.randomUUID().toString.replace("-", "")}"
      withProvisionedBootstrapper(tableName, "p", "r", isCurrentMigration = false) { (bootstrapper, _, _) =>
        for res <- bootstrapper.checkMigrations.exit
        yield assertTrue(res.isFailure)
      }
    },
    test("checkMigrations succeeds when isMigrating is false and migrations are current") {
      val tableName = s"bootstrapper_curr_${UUID.randomUUID().toString.replace("-", "")}"
      withProvisionedBootstrapper(tableName, "p", "r", isMigrating = false, isCurrentMigration = true) {
        (bootstrapper, _, _) =>
          for res <- bootstrapper.checkMigrations.exit
          yield assertTrue(res.isSuccess)
      }
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
