package com.sneaksanddata.arcane.framework
package services.merging

import models.ArcaneSchema.toColumnsExpression
import models.ArcaneType.{BooleanType, LongType, StringType}
import models.{Field, MergeKeyField}
import services.consumers.{SynapseLinkBackfillBatch, SynapseLinkBackfillQuery, SynapseLinkMergeBatch}
import utils.TestTablePropertiesSettings

import io.trino.jdbc.TrinoDriver
import org.scalatest.Assertion
import org.scalatest.flatspec.{AnyFlatSpec, AsyncFlatSpec}
import org.scalatest.matchers.should.Matchers
import zio.{Runtime, Task, Unsafe, ZIO}

import java.sql.Connection
import java.util.Properties
import scala.concurrent.Future
import scala.io.Source
import scala.util.Using

class JdbcMergeServiceClientTests extends AsyncFlatSpec with Matchers:

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
