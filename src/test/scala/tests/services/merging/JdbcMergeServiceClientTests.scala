package com.sneaksanddata.arcane.framework
package tests.services.merging

import models.app.StreamContext
import models.batches.SynapseLinkMergeBatch
import models.schemas.ArcaneType.{BooleanType, LongType, StringType}
import models.schemas.{ArcaneSchema, Field, MergeKeyField}
import models.settings.JdbcMergeServiceClientSettings
import services.base.SchemaProvider
import services.caching.schema_cache.MutableSchemaCache
import services.filters.FieldsFilteringService
import services.merging.*
import services.merging.maintenance.{
  JdbcOptimizationRequest,
  JdbcOrphanFilesExpirationRequest,
  JdbcSnapshotExpirationRequest
}
import services.metrics.{ArcaneDimensionsProvider, DeclaredMetrics}
import tests.services.connectors.mssql.MsSqlReaderTests.suite
import tests.services.merging.JdbcMergeServiceClientTests.test
import tests.shared.{TestBackfillTableSettings, TestTablePropertiesSettings, TestTargetTableSettings}

import io.trino.jdbc.TrinoDriver
import org.scalatestplus.easymock.EasyMockSugar
import org.scalatestplus.easymock.EasyMockSugar.mock
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Scope, Task, Unsafe, ZIO}

import java.sql.Connection
import java.util.Properties

object JdbcMergeServiceClientTests extends ZIOSpecDefault:
  private val connectionUri = "jdbc:trino://localhost:8080/iceberg/test?user=test"
  private val schema = MergeKeyField :: Field("versionnumber", LongType) :: Field("IsDelete", BooleanType) :: Field(
    "colA",
    StringType
  ) :: Field("colB", StringType) :: Field("Id", StringType) :: Nil

  private val schemaProviderMock         = mock[SchemaProvider[ArcaneSchema]]
  private val fieldsFilteringServiceMock = mock[FieldsFilteringService]

  private val streamContext = new StreamContext:
    override val streamId: String       = "test"
    override val IsBackfilling: Boolean = false
    override val streamKind: String     = "test"

  private val options = new JdbcMergeServiceClientSettings:
    /** The connection URL.
      */
    override val connectionUrl: String = connectionUri

  private def getJdbcMergeServiceClient(schemaProviderFactory: Option[SchemaProviderFactory]) =
    new JdbcMergeServiceClient(
      options,
      TestTargetTableSettings,
      TestBackfillTableSettings,
      streamContext,
      schemaProviderMock,
      fieldsFilteringServiceMock,
      TestTablePropertiesSettings,
      MutableSchemaCache(),
      schemaProviderFactory,
      DeclaredMetrics(ArcaneDimensionsProvider(streamContext))
    )

  private def getConnection: Task[Connection] =
    for
      driver     <- ZIO.succeed(new TrinoDriver())
      connection <- ZIO.attemptBlocking(driver.connect(connectionUri, new Properties()))
    yield connection

  private def createTable(tableName: String, connection: Connection): Task[Unit] =
    for
      statement <- ZIO.attemptBlocking(connection.createStatement())

      dropTableStatement = s"DROP TABLE IF EXISTS iceberg.test.$tableName"
      _ <- ZIO.attemptBlocking(statement.execute(dropTableStatement))
      createTableStatement =
        s"CREATE TABLE IF NOT EXISTS iceberg.test.$tableName (ARCANE_MERGE_KEY VARCHAR, versionnumber BIGINT, IsDelete BOOLEAN, colA VARCHAR, colB VARCHAR, Id VARCHAR)"
      _ <- ZIO.attemptBlocking(statement.execute(createTableStatement))
    yield ()

  private def insertValues(tableName: String, connection: Connection): Task[Unit] =
    for
      updateStatement <- ZIO.attemptBlocking(connection.createStatement())
      _ <- ZIO.foreach(1 to 10) { i =>
        val insertCmd =
          s"insert into iceberg.test.$tableName (ARCANE_MERGE_KEY, versionnumber, IsDelete, colA, colB, Id) values ('$i', $i, false, '$i', '$i', '$i')"
        ZIO.attemptBlocking(updateStatement.execute(insertCmd))
      }
    yield ()

  private def setupTable(tableName: String): Task[Unit] =
    for _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie) {
        connection =>
          for
            _ <- createTable(tableName, connection)
            _ <- createTable(s"staged_$tableName", connection)
            _ <- insertValues(s"staged_$tableName", connection)
          yield ()
      }
    yield ()

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("JdbcMergeServiceClientTests")(
    test("applies a batch to target table") {
      for
        tableName <- ZIO.succeed("table_a")
        batch <- ZIO.succeed(
          SynapseLinkMergeBatch(s"test.staged_$tableName", schema, s"test.$tableName", TestTablePropertiesSettings, None)
        )
        _ <- setupTable(tableName)

        connection <- getConnection
        mergeServiceClient = getJdbcMergeServiceClient(None)
        _  <- mergeServiceClient.applyBatch(batch)
        rs <- ZIO.attemptBlocking(connection.createStatement().executeQuery(s"SELECT count(1) FROM ${batch.name}"))
        _  <- ZIO.attemptBlocking(rs.next())
      yield assertTrue(rs.getInt(1) == 10)
    },
    test("disposes of a batch") {
      for
        tableName <- ZIO.succeed("table_disposed")
        batch <- ZIO.succeed(
          SynapseLinkMergeBatch(s"test.staged_$tableName", schema, s"test.$tableName", TestTablePropertiesSettings, None)
        )
        _ <- setupTable(tableName)

        connection <- getConnection
        mergeServiceClient = getJdbcMergeServiceClient(None)
        _  <- mergeServiceClient.disposeBatch(batch)
        rs <- ZIO.attemptBlocking(connection.getMetaData.getTables(null, null, s"staged_$tableName", null))
        stagingTableExists <- ZIO.attemptBlocking(rs.next())
      yield assertTrue(!stagingTableExists)
    },
    test("optimizes a table") {
      for
        tableName <- ZIO.succeed("table_optimized")
        batch <- ZIO.succeed(
          SynapseLinkMergeBatch(s"test.staged_$tableName", schema, s"test.$tableName", TestTablePropertiesSettings, None)
        )
        _ <- setupTable(tableName)

        request            = Some(JdbcOptimizationRequest(tableName, 10, "1MB", 9))
        mergeServiceClient = getJdbcMergeServiceClient(None)
        result <- mergeServiceClient.optimizeTable(request)
      yield assertTrue(!result.skipped)
    },
    test("runs expireSnapshots on a table") {
      for
        tableName <- ZIO.succeed("table_snapshot_expire")
        batch <- ZIO.succeed(
          SynapseLinkMergeBatch(s"test.staged_$tableName", schema, s"test.$tableName", TestTablePropertiesSettings, None)
        )
        _ <- setupTable(tableName)

        request            = Some(JdbcSnapshotExpirationRequest(tableName, 10, "8d", 9))
        mergeServiceClient = getJdbcMergeServiceClient(None)
        result <- mergeServiceClient.expireSnapshots(request)
      yield assertTrue(!result.skipped)
    },
    test("runs expireOrphanFiles on a table") {
      for
        tableName <- ZIO.succeed("table_orphan_files_expire")
        batch <- ZIO.succeed(
          SynapseLinkMergeBatch(s"test.staged_$tableName", schema, s"test.$tableName", TestTablePropertiesSettings, None)
        )
        _ <- setupTable(tableName)

        request            = Some(JdbcOrphanFilesExpirationRequest(tableName, 10, "8d", 9))
        mergeServiceClient = getJdbcMergeServiceClient(None)
        result <- mergeServiceClient.expireOrphanFiles(request)
      yield assertTrue(!result.skipped)
    },
    test("performs schema migrations") {
      for
        tableName <- ZIO.succeed("table_schema_migrated")
        batch <- ZIO.succeed(
          SynapseLinkMergeBatch(s"test.staged_$tableName", schema, s"test.$tableName", TestTablePropertiesSettings, None)
        )
        _ <- setupTable(tableName)
        updatedSchema = MergeKeyField :: Field("versionnumber", LongType) :: Field("IsDelete", BooleanType) ::
          Field("colA", StringType) :: Field("colB", StringType) :: Field("Id", StringType) ::
          Field("new_column", StringType) :: Nil

        mergeServiceClient = getJdbcMergeServiceClient(None)
        result <- mergeServiceClient.migrateSchema(updatedSchema, tableName)

        newSchema <- mergeServiceClient.getSchema(tableName)
      yield assertTrue(newSchema == updatedSchema)
    },
    test("reads schema once during schema migrations") {
      var callCount = 0

      def schemaProviderFactory(name: String, connection: Connection) =
        callCount = callCount + 1
        new JdbcSchemaProvider(name, connection)

      for
        tableName <- ZIO.succeed("table_schema_migrations")
        batch <- ZIO.succeed(
          SynapseLinkMergeBatch(s"test.staged_$tableName", schema, s"test.$tableName", TestTablePropertiesSettings, None)
        )
        _ <- setupTable(tableName)
        updatedSchema = MergeKeyField :: Field("versionnumber", LongType) :: Field("IsDelete", BooleanType) ::
          Field("colA", StringType) :: Field("colB", StringType) :: Field("Id", StringType) ::
          Field("new_column", StringType) :: Nil

        mergeServiceClient = getJdbcMergeServiceClient(Some(schemaProviderFactory))
        // The schema provider should be called twice, once for the original schema and once for the updated schema
        _ <- mergeServiceClient.migrateSchema(updatedSchema, tableName) // First and second calls here
        _ <- mergeServiceClient.migrateSchema(updatedSchema, tableName)
        _ <- mergeServiceClient.migrateSchema(updatedSchema, tableName)
        _ <- mergeServiceClient.migrateSchema(updatedSchema, tableName)
        _ <- mergeServiceClient.getSchema(tableName)

        newSchema <- mergeServiceClient.getSchema(tableName)
      yield assertTrue(callCount == 2)
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
