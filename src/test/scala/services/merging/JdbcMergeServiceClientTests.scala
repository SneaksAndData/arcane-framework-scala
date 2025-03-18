package com.sneaksanddata.arcane.framework
package services.merging

import services.consumers.SynapseLinkMergeBatch
import utils.{TestBackfillTableSettings, TestStagingDataSettings, TestTablePropertiesSettings, TestTargetTableSettings}

import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, Field, MergeKeyField}
import com.sneaksanddata.arcane.framework.models.ArcaneType.{BooleanType, LongType, StringType}
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.services.filters.FieldsFilteringService
import com.sneaksanddata.arcane.framework.services.merging.models.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
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
    

  private val options = new JdbcMergeServiceClientOptions:
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
    val createTableStatement = s"CREATE TABLE IF NOT EXISTS test.$tableName (ARCANE_MERGE_KEY VARCHAR, ARCANE_BATCH_ID VARCHAR, versionnumber BIGINT, IsDelete BOOLEAN, colA VARCHAR, colB VARCHAR, Id VARCHAR)"
    statement.execute(createTableStatement)
    connection

  private def insertValues(tableName: String): Unit =
    val driver = new TrinoDriver()
    val connection = driver.connect(connectionUri, new Properties())
    val updateStatement = connection.createStatement()
    for i <- 1 to 10 do
      val insertCmd = s"insert into test.$tableName (ARCANE_MERGE_KEY, ARCANE_BATCH_ID, versionnumber, IsDelete, colA, colB, Id) values ('$i', '$i', $i, false, '$i', '$i', '$i')"
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
      TestStagingDataSettings,
      streamContext,
      schemaProviderMock,
      fieldsFilteringServiceMock,
      TestTablePropertiesSettings,
      MutableSchemaCache(),
      schemaProviderFactory)

  it should "be able to apply a batch to target table" in withTargetTable("table_a") { connection =>
    val batch = SynapseLinkMergeBatch("test.staged_table_a", "table_id", schema, "test.table_a", TestTablePropertiesSettings)
    val mergeServiceClient = getSystemUnderTest(None)

    for _ <- mergeServiceClient.applyBatch(batch)
        rs = connection.createStatement().executeQuery(s"SELECT count(1) FROM ${batch.name}")
        _ = rs.next()
        targetCount = rs.getInt(1)

    // assert that the statement was actually executed
    yield targetCount should be(10)
  }

  it should "should be able to dispose a batch" in withTargetTable("staging_stream_id") { connection =>
    val mergeServiceClient = getSystemUnderTest(None)
    val batch = SynapseLinkMergeBatch("test.staged_table_a", "batch_id", schema, "test.table_a", TestTablePropertiesSettings)

    for _ <- mergeServiceClient.disposeBatch(batch)
        rs = connection.getMetaData.getTables(null, null, "staging_stream_id", null)
        stagingTableExists = rs.next()

    // assert that the statement was actually executed
    yield stagingTableExists should be(true)
  }

  it should "should be able to run optimizeTable queries without errors" in withTargetTable("table_a") { connection =>
    val tableName = "table_a"
    val optimizeThreshold = 10
    val fileSizeThreshold = "1MB"
    val batchIndex = 9
    val request = JdbcOptimizationRequest(tableName, optimizeThreshold, fileSizeThreshold, batchIndex)


    val mergeServiceClient = getSystemUnderTest(None)
    for result <- mergeServiceClient.optimizeTable(request)
    yield result.skipped should be(false)
  }

  it should "should be able to run expireSnapshots queries without errors" in withTargetTable("table_a") { connection =>

    val tableName = "table_a"
    val optimizeThreshold = 10
    val retentionThreshold = "8d"
    val batchIndex = 9
    val request = JdbcSnapshotExpirationRequest(tableName, optimizeThreshold, retentionThreshold, batchIndex)

    val mergeServiceClient = getSystemUnderTest(None)
    for result <- mergeServiceClient.expireSnapshots(request)
    yield result.skipped should be(false)
  }

  it should "should be able to run expireOrphanFiles queries without errors" in withTargetTable("table_a") { connection =>

    val tableName = "table_a"
    val optimizeThreshold = 10
    val retentionThreshold = "8d"
    val batchIndex = 9
    val request = JdbcOrphanFilesExpirationRequest(tableName, optimizeThreshold, retentionThreshold, batchIndex)

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

  it should "should read schema once during schema migrations" in withTargetTable("table_a") { connection =>
    val updatedSchema = MergeKeyField :: Field("versionnumber", LongType) :: Field("IsDelete", BooleanType) ::
      Field("colA", StringType) :: Field("colB", StringType) :: Field("Id", StringType) ::
      Field("new_column", StringType) :: Nil

    var callCount = 0


    def schemaProviderFactory(name: String, connection: Connection) =
      callCount = callCount + 1
      new JdbcSchemaProvider(name, connection)


    val mergeServiceClient = getSystemUnderTest(Some(schemaProviderFactory))

    // The schema provider should be called twice, once for the original schema and once for the updated schema
    for result <- mergeServiceClient.migrateSchema(updatedSchema, "table_a") // First and second calls here
        result <- mergeServiceClient.migrateSchema(updatedSchema, "table_a")
        result <- mergeServiceClient.migrateSchema(updatedSchema, "table_a")
        result <- mergeServiceClient.migrateSchema(updatedSchema, "table_a")
        newSchema <- mergeServiceClient.getSchema("table_a")
    yield callCount should be(2)
  }
