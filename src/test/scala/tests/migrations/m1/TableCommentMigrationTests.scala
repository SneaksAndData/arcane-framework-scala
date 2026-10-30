package com.sneaksanddata.arcane.framework
package tests.migrations.m1

import migrations.m1.TableCommentMigration
import models.app.{PluginStreamContext, StreamDocumentation}
import models.ddl.CreateTableRequest
import models.settings.sink.SinkSettings
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
  TestStreamingSource
}

import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.types.Types
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO}

import java.util.UUID

object TableCommentMigrationTests extends ZIOSpecDefault:

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

  private def withProvisionedTable[R, A](tableName: String, initialProperties: Map[String, String] = Map.empty)(
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
        override val sink: SinkSettings = new TestDynamicSinkSettings(s"demo.test.$tableName")
      result <- ZIO.acquireReleaseWith(
        entityManager.createTable(CreateTableRequest(tableName, tableSchema, true, initialProperties))
      )(_ => entityManager.delete(tableName).orDie) { _ =>
        testLogic(entityManager, propertyManager, context)
      }
    yield result

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("TableCommentMigration")(
    test("updates comment property with fallback values when env vars are unset") {
      val tableName = s"comment_mig_${UUID.randomUUID().toString.replace("-", "")}"
      val migration = new TableCommentMigration()
      val expectedComment =
        s"Table managed by ${StreamDocumentation.fallbackPluginName} ${StreamDocumentation.fallbackGithubRepoLink}"

      withProvisionedTable(tableName) { (_, propertyManager, context) =>
        for
          _          <- migration.execute(propertyManager, context, testMergeServiceClient, TestStreamingSource)
          commentOpt <- propertyManager.getProperty(tableName, "comment")
        yield assertTrue(commentOpt.contains(expectedComment))
      }
    },
    test("updates comment property with configured env vars when present") {
      val tableName       = s"comment_mig_env_${UUID.randomUUID().toString.replace("-", "")}"
      val migration       = new TableCommentMigration()
      val plugin          = "custom-plugin"
      val repo            = "https://github.com/custom/repo"
      val expectedComment = s"Table managed by $plugin $repo"

      withProvisionedTable(tableName, Map("comment" -> "old comment")) { (_, propertyManager, context) =>
        TestSystem.putEnv("STREAMCONTEXT__PLUGIN_NAME", plugin) *>
          TestSystem.putEnv("STREAMCONTEXT__PLUGIN_GITHUB_REPO_LINK", repo) *>
          (for
            _          <- migration.execute(propertyManager, context, testMergeServiceClient, TestStreamingSource)
            commentOpt <- propertyManager.getProperty(tableName, "comment")
          yield assertTrue(commentOpt.contains(expectedComment)))
      }
    },
    test("does not update comment property when comment is already up-to-date") {
      val tableName = s"comment_mig_noop_${UUID.randomUUID().toString.replace("-", "")}"
      val migration = new TableCommentMigration()
      val expectedComment =
        s"Table managed by ${StreamDocumentation.fallbackPluginName} ${StreamDocumentation.fallbackGithubRepoLink}"

      withProvisionedTable(tableName, Map("comment" -> expectedComment)) { (_, propertyManager, context) =>
        for
          _          <- migration.execute(propertyManager, context, testMergeServiceClient, TestStreamingSource)
          commentOpt <- propertyManager.getProperty(tableName, "comment")
        yield assertTrue(commentOpt.contains(expectedComment))
      }
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
