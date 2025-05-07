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
import services.merging.maintenance.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import tests.shared.{TestBackfillTableSettings, TestTablePropertiesSettings, TestTargetTableSettings}

import io.trino.jdbc.TrinoDriver
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.easymock.EasyMockSugar
import zio.{Runtime, Task, Unsafe}

import java.sql.Connection
import java.util.Properties
import scala.concurrent.Future


class JdbcMergeServiceClientTests extends AsyncFlatSpec with Matchers with EasyMockSugar:

  private val connectionUri = "jdbc:trino://localhost:8080/iceberg/test?user=test"
  private val schema = MergeKeyField :: Field("versionnumber", LongType) :: Field("IsDelete", BooleanType) :: Field("colA", StringType) :: Field("colB", StringType) :: Field("Id", StringType) :: Nil
  private val runtime = Runtime.default

  private val schemaProviderMock = mock[SchemaProvider[ArcaneSchema]]
  private val fieldsFilteringServiceMock = mock[FieldsFilteringService]
  private val streamContext = new StreamContext:
    override val streamId: String = "test"
    override val IsBackfilling: Boolean = true
    override val streamKind: String = "test"
    

  private val options = new JdbcMergeServiceClientSettings:
    /**
     * The connection URL.
     */
    override val connectionUrl: String = connectionUri

  private def createTable(tableName: String): Connection  =
    val driver = new TrinoDriver()
    val connection = driver.connect(connectionUri, new Properties())
    val statement = connection.createStatement()

    val dropTableStatement = s"DROP TABLE IF EXISTS test.$tableName"
    statement.execute(dropTableStatement)
    val createTableStatement = s"CREATE TABLE IF NOT EXISTS test.$tableName (ARCANE_MERGE_KEY VARCHAR, versionnumber BIGINT, IsDelete BOOLEAN, colA VARCHAR, colB VARCHAR, Id VARCHAR)"
    statement.execute(createTableStatement)
    connection

  private def insertValues(tableName: String): Unit =
    val driver = new TrinoDriver()
    val connection = driver.connect(connectionUri, new Properties())
    val updateStatement = connection.createStatement()
    for i <- 1 to 10 do
      val insertCmd = s"insert into test.$tableName (ARCANE_MERGE_KEY, versionnumber, IsDelete, colA, colB, Id) values ('$i', $i, false, '$i', '$i', '$i')"
      updateStatement.execute(insertCmd)

  private def withTargetTable(tableName: String)(test: Connection => Task[Assertion]): Future[Assertion] =
    createTable(tableName)
    val connection = createTable(s"staged_$tableName")
    insertValues(s"staged_$tableName")
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(test(connection)))

  private def withTargetAndArchiveTables(tableName: String)(test: Connection => Task[Assertion]): Future[Assertion] =
    createTable(tableName)
    createTable(s"archive_$tableName")
    val connection = createTable(s"staged_$tableName")
    insertValues(s"staged_$tableName")

    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(test(connection)))

  private def getSystemUnderTest(schemaProviderFactory: Option[SchemaProviderFactory]) = new JdbcMergeServiceClient(options,
      TestTargetTableSettings,
      TestBackfillTableSettings,
      streamContext,
      schemaProviderMock,
      fieldsFilteringServiceMock,
      TestTablePropertiesSettings,
      MutableSchemaCache(),
      schemaProviderFactory)

  it should "should be able to apply a batch to target table" in withTargetTable("table_a") { connection =>
    val batch = SynapseLinkMergeBatch("test.staged_table_a", schema, "test.table_a", TestTablePropertiesSettings)
    val mergeServiceClient = getSystemUnderTest(None)

    for _ <- mergeServiceClient.applyBatch(batch)
        rs = connection.createStatement().executeQuery(s"SELECT count(1) FROM ${batch.name}")
        _ = rs.next()
        targetCount = rs.getInt(1)

    // assert that the statement was actually executed
    yield targetCount should be(10)
  }

  it should "should be able to dispose a batch" in withTargetTable("table_a") { connection =>
    val mergeServiceClient = getSystemUnderTest(None)
    val batch = SynapseLinkMergeBatch("test.staged_table_a", schema, "test.table_a", TestTablePropertiesSettings)

    for _ <- mergeServiceClient.disposeBatch(batch)
        rs = connection.getMetaData.getTables(null, null, "staged_table_a", null)
        stagingTableExists = rs.next()

    // assert that the statement was actually executed
    yield stagingTableExists should be(false)
  }

  it should "should be able to run optimizeTable queries without errors" in withTargetTable("table_a") { connection =>
    val tableName = "table_a"
    val optimizeThreshold = 10
    val fileSizeThreshold = "1MB"
    val batchIndex = 9
    val request = Some(JdbcOptimizationRequest(tableName, optimizeThreshold, fileSizeThreshold, batchIndex))


    val mergeServiceClient = getSystemUnderTest(None)
    for result <- mergeServiceClient.optimizeTable(request)
    yield result.skipped should be(false)
  }

  it should "should be able to run expireSnapshots queries without errors" in withTargetTable("table_a") { connection =>

    val tableName = "table_a"
    val optimizeThreshold = 10
    val retentionThreshold = "8d"
    val batchIndex = 9
    val request = Some(JdbcSnapshotExpirationRequest(tableName, optimizeThreshold, retentionThreshold, batchIndex))

    val mergeServiceClient = getSystemUnderTest(None)
    for result <- mergeServiceClient.expireSnapshots(request)
    yield result.skipped should be(false)
  }

  it should "should be able to run expireOrphanFiles queries without errors" in withTargetTable("table_a") { connection =>

    val tableName = "table_a"
    val optimizeThreshold = 10
    val retentionThreshold = "8d"
    val batchIndex = 9
    val request = Some(JdbcOrphanFilesExpirationRequest(tableName, optimizeThreshold, retentionThreshold, batchIndex))

    val mergeServiceClient = getSystemUnderTest(None)
    for result <- mergeServiceClient.expireOrphanFiles(request)
    yield result.skipped should be(false)
  }

  it should "should be able to perform schema migrations" in withTargetTable("table_a") { connection =>
    val updatedSchema = MergeKeyField :: Field("versionnumber", LongType) :: Field("IsDelete", BooleanType) ::
      Field("colA", StringType) :: Field("colB", StringType) :: Field("Id", StringType) ::
      Field("new_column", StringType) :: Nil

    val mergeServiceClient = getSystemUnderTest(None)
    for result <- mergeServiceClient.migrateSchema(updatedSchema, "table_a")
        newSchema <- mergeServiceClient.getSchema("table_a")
    yield newSchema should contain theSameElementsAs updatedSchema
  }

  it should "read schema once during schema migrations" in withTargetTable("table_a") { connection =>
    val updatedSchema = MergeKeyField :: Field("versionnumber", LongType) :: Field("IsDelete", BooleanType) ::
      Field("colA", StringType) :: Field("colB", StringType) :: Field("Id", StringType) ::
      Field("new_column", StringType) :: Nil

    var callCount = 0


    def schemaProviderFactory(name: String, connection: Connection) =
      callCount = callCount + 1
      new JdbcSchemaProvider(name, connection)


    val mergeServiceClient = getSystemUnderTest(Some(schemaProviderFactory))

    // The schema provider should be called twice, once for the original schema and once for the updated schema
    for _ <- mergeServiceClient.migrateSchema(updatedSchema, "table_a") // First and second calls here
        _ <- mergeServiceClient.migrateSchema(updatedSchema, "table_a")
        _ <- mergeServiceClient.migrateSchema(updatedSchema, "table_a")
        _ <- mergeServiceClient.migrateSchema(updatedSchema, "table_a")
        _ <- mergeServiceClient.getSchema("table_a")
    yield callCount should be(2)
  }
