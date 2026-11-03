package com.sneaksanddata.arcane.framework
package tests.migrations.m1

import migrations.m1.FieldSelectorSchemaMigration
import models.app.PluginStreamContext
import models.ddl.CreateTableRequest
import models.schemas.{ArcaneSchema, Field, MergeKeyField, VersionField, given_CanAdd_ArcaneSchema}
import models.schemas.ArcaneType.{IntType, StringType}
import models.settings.sink.SinkSettings
import models.settings.sources.{
  BufferingStrategy,
  SourceBufferingSettings,
  SourceSettings,
  StreamSourceSettings,
  Unbounded,
  UnboundedImpl
}
import models.settings.sources.modification.*
import services.base.StreamingSource
import services.iceberg.base.SinkPropertyManager
import services.iceberg.{IcebergSinkEntityManager, IcebergSinkTablePropertyManager}
import services.merging.JdbcMergeServiceClient
import services.metrics.DeclaredMetrics
import services.streaming.base.TimestampOnlyWatermark
import tests.shared.{
  IcebergCatalogInfo,
  IcebergUtil,
  TestDynamicSinkSettings,
  TestJdbcMergeServiceClientSettings,
  TestPluginStreamContextImpl
}

import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.types.Types
import zio.stream.ZStream
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, Task, ZIO}

import java.util.UUID

object FieldSelectorSchemaMigrationTests extends ZIOSpecDefault:

  private val testMergeServiceClient = new JdbcMergeServiceClient(
    TestJdbcMergeServiceClientSettings,
    "iceberg",
    "test",
    DeclaredMetrics(),
    false
  )

  private def createStreamingSource(schema: ArcaneSchema): StreamingSource =
    new StreamingSource:
      override type SchemaType    = ArcaneSchema
      override type WatermarkType = TimestampOnlyWatermark
      override type ShardMetadata = String
      override def getSchema: Task[SchemaType]                                   = ZIO.succeed(schema)
      override def getShards(rangeStart: WatermarkType, rangeEnd: WatermarkType) = ZStream.empty
      override def deleteShards(prefix: String): Task[Unit]                      = ZIO.unit
      override def empty: SchemaType                                             = ArcaneSchema.empty()

  private def withProvisionedTable[R, A](tableName: String, schema: IcebergSchema, mods: DataRowModificationSettings)(
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
        override val sink: SinkSettings = new TestDynamicSinkSettings(s"iceberg.test.$tableName")
        override val source: StreamSourceSettings = new StreamSourceSettings {
          override type SourceSettingsType = SourceSettings
          override val configuration: SourceSettings = new SourceSettings {}
          override val buffering: SourceBufferingSettings = new SourceBufferingSettings {
            override val bufferingStrategy: BufferingStrategy = UnboundedImpl(Unbounded())
            override val bufferingEnabled: Boolean            = false
            override type MergeableFrom = this.type
            override type MergeResult   = this.type
            override def merge(overrides: Option[MergeableFrom]): MergeResult = ???
          }
          override val modifications: DataRowModificationSettings = mods
        }
      result <- ZIO.acquireReleaseWith(
        entityManager.createTable(CreateTableRequest(tableName, schema, true))
      )(_ => entityManager.delete(tableName).orDie) { _ =>
        testLogic(entityManager, propertyManager, context)
      }
    yield result

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("FieldSelectorSchemaMigrationTests")(
    test("drops excess columns from sink table when fieldSelector is configured") {
      val tableName = s"fs_mig_${UUID.randomUUID().toString.replace("-", "")}"
      // Sink table has extra columns colB and colC which were leaked in 2.3
      val sinkSchema = new IcebergSchema(
        Types.NestedField.optional(1, MergeKeyField.name, Types.StringType.get()),
        Types.NestedField.optional(2, VersionField.name, Types.LongType.get()),
        Types.NestedField.optional(3, "colA", Types.StringType.get()),
        Types.NestedField.optional(4, "colB", Types.StringType.get()),
        Types.NestedField.optional(5, "colC", Types.IntegerType.get())
      )

      // Expected source schema after fieldSelector only has colA, plus essential fields
      val sourceSchema: ArcaneSchema = Seq(
        MergeKeyField,
        VersionField,
        Field("colA", StringType)
      )
      val source = createStreamingSource(sourceSchema)

      val mods = DefaultDataRowModificationSettings(
        SupportedModifications(
          fieldSelector = Some(FieldSelectorSetting(include = Some(IncludeFieldSelector(Set("colA")))))
        )
      )

      val migration = new FieldSelectorSchemaMigration()

      withProvisionedTable(tableName, sinkSchema, mods) { (_, propertyManager, context) =>
        for
          columnsBefore <- propertyManager.getTableSchema(tableName).map(_.columns())
          _             <- migration.execute(propertyManager, context, testMergeServiceClient, source)
          columnsAfter  <- propertyManager.getTableSchema(tableName).map(_.columns())
        yield assertTrue(
          columnsBefore.stream().anyMatch(_.name().equalsIgnoreCase("colB")),
          columnsBefore.stream().anyMatch(_.name().equalsIgnoreCase("colC")),
          !columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase("colB")),
          !columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase("colC")),
          columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase("colA")),
          columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase(MergeKeyField.name)),
          columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase(VersionField.name))
        )
      }
    },
    test("does not drop essential fields even if not explicitly present in source schema") {
      val tableName = s"fs_mig_ess_${UUID.randomUUID().toString.replace("-", "")}"
      val sinkSchema = new IcebergSchema(
        Types.NestedField.optional(1, MergeKeyField.name, Types.StringType.get()),
        Types.NestedField.optional(2, VersionField.name, Types.LongType.get()),
        Types.NestedField.optional(3, "colA", Types.StringType.get()),
        Types.NestedField.optional(4, "leakedCol", Types.StringType.get())
      )

      // Source schema doesn't even list essential fields, e.g. raw source
      val sourceSchema: ArcaneSchema = Seq(Field("colA", StringType))
      val source                     = createStreamingSource(sourceSchema)

      val mods = DefaultDataRowModificationSettings(
        SupportedModifications(
          fieldSelector = Some(FieldSelectorSetting(include = Some(IncludeFieldSelector(Set("colA")))))
        )
      )

      val migration = new FieldSelectorSchemaMigration()

      withProvisionedTable(tableName, sinkSchema, mods) { (_, propertyManager, context) =>
        for
          _            <- migration.execute(propertyManager, context, testMergeServiceClient, source)
          columnsAfter <- propertyManager.getTableSchema(tableName).map(_.columns())
        yield assertTrue(
          !columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase("leakedCol")),
          columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase("colA")),
          columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase(MergeKeyField.name)),
          columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase(VersionField.name))
        )
      }
    },
    test("is a no-op when no fieldSelector is configured") {
      val tableName = s"fs_mig_noop_${UUID.randomUUID().toString.replace("-", "")}"
      val sinkSchema = new IcebergSchema(
        Types.NestedField.optional(1, MergeKeyField.name, Types.StringType.get()),
        Types.NestedField.optional(2, VersionField.name, Types.LongType.get()),
        Types.NestedField.optional(3, "colA", Types.StringType.get()),
        Types.NestedField.optional(4, "colB", Types.StringType.get())
      )

      val sourceSchema: ArcaneSchema = Seq(Field("colA", StringType))
      val source                     = createStreamingSource(sourceSchema)

      val mods      = DefaultDataRowModificationSettings(SupportedModifications(None, None))
      val migration = new FieldSelectorSchemaMigration()

      withProvisionedTable(tableName, sinkSchema, mods) { (_, propertyManager, context) =>
        for
          columnsBefore <- propertyManager.getTableSchema(tableName).map(_.columns())
          _             <- migration.execute(propertyManager, context, testMergeServiceClient, source)
          columnsAfter  <- propertyManager.getTableSchema(tableName).map(_.columns())
        yield assertTrue(
          columnsAfter.size() == columnsBefore.size(),
          columnsAfter.stream().anyMatch(_.name().equalsIgnoreCase("colB"))
        )
      }
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
