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
import services.merging.maintenance.JdbcOptimizationRequest
import services.metrics.{ArcaneDimensionsProvider, DeclaredMetrics}
import tests.services.connectors.mssql.MsSqlConnectionTests.suite
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

  private def getJdbcMergeServiceClient(schemaProviderFactory: Option[SchemaProviderFactory]) = new JdbcMergeServiceClient(
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
      statement  <- ZIO.attemptBlocking(connection.createStatement())

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

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("JdbcMergeServiceClientTests")(
    test("applies a batch to target table") {
      for
        tableName <- ZIO.succeed("table_a")
        batch <- ZIO.succeed(SynapseLinkMergeBatch(s"test.staged_$tableName", schema, s"test.$tableName", TestTablePropertiesSettings))
        connection <- getConnection
        _ <- createTable(tableName, connection)
        _ <- createTable(s"staged_$tableName", connection)
        _ <- insertValues(s"staged_$tableName", connection)
        mergeServiceClient = getJdbcMergeServiceClient(None)
        _ <- mergeServiceClient.applyBatch(batch)
        rs <- ZIO.attemptBlocking(connection.createStatement().executeQuery(s"SELECT count(1) FROM ${batch.name}"))
        _           <- ZIO.attemptBlocking(rs.next())
      yield assertTrue(rs.getInt(1) == 10)
    },
    test("disposes of a batch") {
      for
        tableName <- ZIO.succeed("table_disposed")
        batch <- ZIO.succeed(SynapseLinkMergeBatch(s"test.staged_$tableName", schema, s"test.$tableName", TestTablePropertiesSettings))
        connection <- getConnection
        _ <- createTable(tableName, connection)
        _ <- createTable(s"staged_$tableName", connection)
        _ <- insertValues(s"staged_$tableName", connection)
        mergeServiceClient = getJdbcMergeServiceClient(None)
        _ <- mergeServiceClient.disposeBatch(batch)
        rs <- ZIO.attemptBlocking(connection.getMetaData.getTables(null, null, s"staged_$tableName", null))
        stagingTableExists           <- ZIO.attemptBlocking(rs.next())
      yield assertTrue(!stagingTableExists)
    },
    test("optimizes a table") {
      for
        tableName <- ZIO.succeed("table_optimized")
        batch <- ZIO.succeed(SynapseLinkMergeBatch(s"test.staged_$tableName", schema, s"test.$tableName", TestTablePropertiesSettings))
        connection <- getConnection
        _ <- createTable(tableName, connection)
        _ <- createTable(s"staged_$tableName", connection)
        _ <- insertValues(s"staged_$tableName", connection)

        request           = Some(JdbcOptimizationRequest(tableName, 10, "1MB", 9))
        mergeServiceClient = getJdbcMergeServiceClient(None)
        result <- mergeServiceClient.optimizeTable(request)
      yield assertTrue(!result.skipped)
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock

//
//  it should "should be able to run optimizeTable queries without errors" in withTargetTable("table_a") { connection =>
//    val tableName         = "table_a"
//    val optimizeThreshold = 10
//    val fileSizeThreshold = "1MB"
//    val batchIndex        = 9
//    val request           = Some(JdbcOptimizationRequest(tableName, optimizeThreshold, fileSizeThreshold, batchIndex))
//
//    val mergeServiceClient = getSystemUnderTest(None)
//    for result <- mergeServiceClient.optimizeTable(request)
//    yield result.skipped should be(false)
//  }
//
//  it should "should be able to run expireSnapshots queries without errors" in withTargetTable("table_a") { connection =>
//
//    val tableName          = "table_a"
//    val optimizeThreshold  = 10
//    val retentionThreshold = "8d"
//    val batchIndex         = 9
//    val request = Some(JdbcSnapshotExpirationRequest(tableName, optimizeThreshold, retentionThreshold, batchIndex))
//
//    val mergeServiceClient = getSystemUnderTest(None)
//    for result <- mergeServiceClient.expireSnapshots(request)
//    yield result.skipped should be(false)
//  }
//
//  it should "should be able to run expireOrphanFiles queries without errors" in withTargetTable("table_a") {
//    connection =>
//
//      val tableName          = "table_a"
//      val optimizeThreshold  = 10
//      val retentionThreshold = "8d"
//      val batchIndex         = 9
//      val request = Some(JdbcOrphanFilesExpirationRequest(tableName, optimizeThreshold, retentionThreshold, batchIndex))
//
//      val mergeServiceClient = getSystemUnderTest(None)
//      for result <- mergeServiceClient.expireOrphanFiles(request)
//      yield result.skipped should be(false)
//  }
//
//  it should "should be able to perform schema migrations" in withTargetTable("table_a") { connection =>
//    val updatedSchema = MergeKeyField :: Field("versionnumber", LongType) :: Field("IsDelete", BooleanType) ::
//      Field("colA", StringType) :: Field("colB", StringType) :: Field("Id", StringType) ::
//      Field("new_column", StringType) :: Nil
//
//    val mergeServiceClient = getSystemUnderTest(None)
//    for
//      result    <- mergeServiceClient.migrateSchema(updatedSchema, "table_a")
//      newSchema <- mergeServiceClient.getSchema("table_a")
//    yield newSchema should contain theSameElementsAs updatedSchema
//  }
//
//  it should "read schema once during schema migrations" in withTargetTable("table_a") { connection =>
//    val updatedSchema = MergeKeyField :: Field("versionnumber", LongType) :: Field("IsDelete", BooleanType) ::
//      Field("colA", StringType) :: Field("colB", StringType) :: Field("Id", StringType) ::
//      Field("new_column", StringType) :: Nil
//
//    var callCount = 0
//
//    def schemaProviderFactory(name: String, connection: Connection) =
//      callCount = callCount + 1
//      new JdbcSchemaProvider(name, connection)
//
//    val mergeServiceClient = getSystemUnderTest(Some(schemaProviderFactory))
//
//    // The schema provider should be called twice, once for the original schema and once for the updated schema
//    for
//      _ <- mergeServiceClient.migrateSchema(updatedSchema, "table_a") // First and second calls here
//      _ <- mergeServiceClient.migrateSchema(updatedSchema, "table_a")
//      _ <- mergeServiceClient.migrateSchema(updatedSchema, "table_a")
//      _ <- mergeServiceClient.migrateSchema(updatedSchema, "table_a")
//      _ <- mergeServiceClient.getSchema("table_a")
//    yield callCount should be(2)
//  }
