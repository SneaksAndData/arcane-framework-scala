package com.sneaksanddata.arcane.framework
package services.merging

import services.consumers.SynapseLinkMergeBatch
import utils.TestTablePropertiesSettings

import com.sneaksanddata.arcane.framework.models.{Field, MergeKeyField}
import com.sneaksanddata.arcane.framework.models.ArcaneType.{BooleanType, LongType, StringType}
import com.sneaksanddata.arcane.framework.models.ArcaneSchema.toColumnsExpression
import com.sneaksanddata.arcane.framework.services.merging.models.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import io.trino.jdbc.TrinoDriver
import org.scalatest.Assertion
import org.scalatest.flatspec.{AnyFlatSpec, AsyncFlatSpec}
import org.scalatest.matchers.should.Matchers
import zio.{Runtime, Task, Unsafe, ZIO}

import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.concurrent.Future
import scala.io.Source
import scala.util.Using


class JdbcMergeServiceClientTests extends AnyFlatSpec with Matchers:

  private val connectionUri = "jdbc:trino://localhost:8080/iceberg/test?user=test"
  private val schema = MergeKeyField :: Field("versionnumber", LongType) :: Field("IsDelete", BooleanType) :: Field("colA", StringType) :: Field("colB", StringType) :: Field("Id", StringType) :: Nil
  private val runtime = Runtime.default

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

  private def withTargetTable(tableName: String)(test: Connection => Task[Assertion]): Assertion =
    createTable(tableName)
    val connection = createTable(s"staged_$tableName")
    insertValues(s"staged_$tableName")
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.run(test(connection)).getOrThrowFiberFailure())

  private def withTargetAndArchiveTables(tableName: String)(test: Connection => Task[Assertion]): Assertion =
    createTable(tableName)
    createTable(s"archive_$tableName")
    val connection = createTable(s"staged_$tableName")
    insertValues(s"staged_$tableName")

    Unsafe.unsafe(implicit unsafe => runtime.unsafe.run(test(connection)).getOrThrowFiberFailure())

  it should "should be able to apply a batch to target table" in withTargetTable("table_a") { connection =>
    val mergeServiceClient = new JdbcMergeServiceClient(options)
    val batch = SynapseLinkMergeBatch("test.staged_table_a", schema, "test.table_a", "test.archive_table_a", TestTablePropertiesSettings)

    for _ <- mergeServiceClient.applyBatch(batch)
        rs = connection.createStatement().executeQuery(s"SELECT count(1) FROM ${batch.name}")
        _ = rs.next()
        targetCount = rs.getInt(1)

    // assert that the statement was actually executed
    yield targetCount should be(10)
  }

  it should "should be able to archive a batch to archive table" in withTargetAndArchiveTables("table_a") { connection =>
    val mergeServiceClient = new JdbcMergeServiceClient(options)
    val batch = SynapseLinkMergeBatch("test.staged_table_a", schema, "test.table_a", "test.archive_table_a", TestTablePropertiesSettings)

    for _ <- mergeServiceClient.archiveBatch(batch, batch.schema)
        rs = connection.createStatement().executeQuery(s"SELECT count(1) FROM archive_table_a")
        _ = rs.next()
        targetCount = rs.getInt(1)

    // assert that the statement was actually executed
    yield targetCount should be(10)
  }

  it should "should be able to dispose a batch" in withTargetTable("table_a") { connection =>
    val mergeServiceClient = new JdbcMergeServiceClient(options)
    val batch = SynapseLinkMergeBatch("test.staged_table_a", schema, "test.table_a", "test.archive_table_a", TestTablePropertiesSettings)

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
    val request = JdbcOptimizationRequest(tableName, optimizeThreshold, fileSizeThreshold, batchIndex)


    val mergeServiceClient = new JdbcMergeServiceClient(options)
      for result <- mergeServiceClient.optimizeTable(request)
      yield result.skipped should be(false)
  }

  it should "should be able to run expireSnapshots queries without errors" in withTargetTable("table_a") { connection =>

    val tableName = "table_a"
    val optimizeThreshold = 10
    val retentionThreshold = "8d"
    val batchIndex = 9
    val request = JdbcSnapshotExpirationRequest(tableName, optimizeThreshold, retentionThreshold, batchIndex)

    val mergeServiceClient = new JdbcMergeServiceClient(options)
    for result <- mergeServiceClient.expireSnapshots(request)
      yield result.skipped should be(false)
  }

  it should "should be able to run expireOrphanFiles queries without errors" in withTargetTable("table_a") { connection =>

    val tableName = "table_a"
    val optimizeThreshold = 10
    val retentionThreshold = "8d"
    val batchIndex = 9
    val request = JdbcOrphanFilesExpirationRequest(tableName, optimizeThreshold, retentionThreshold, batchIndex)

    val mergeServiceClient = new JdbcMergeServiceClient(options)
    for result <- mergeServiceClient.expireOrphanFiles(request)
      yield result.skipped should be(false)
  }
