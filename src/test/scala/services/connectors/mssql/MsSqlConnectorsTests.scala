package com.sneaksanddata.arcane.framework
package services.connectors.mssql

import models.{ArcaneSchemaField, DataCell}
import models.ArcaneType.{IntType, LongType, StringType}
import services.connectors.mssql.util.TestConnectionInfo
import services.mssql.query.{LazyQueryResult, ScalarQueryResult}
import services.mssql.{ConnectionOptions, MsSqlConnection, QueryProvider}

import com.microsoft.sqlserver.jdbc.SQLServerDriver
import org.scalatest.*
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.*
import zio.{Runtime, Unsafe, ZIO}

import java.sql.Connection
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime}
import java.util.Properties
import scala.List
import scala.concurrent.Future
import scala.language.postfixOps

class MsSqlConnectorsTests extends flatspec.AsyncFlatSpec with Matchers:
  private val runtime = Runtime.default
  
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  
  /// To avoid mocking current date/time  we use the formatter that will always return the same value
  private implicit val constantFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("111")

  val connectionUrl = "jdbc:sqlserver://localhost;encrypt=true;trustServerCertificate=true;username=sa;password=tMIxN11yGZgMC;databaseName=arcane"

  def createDb(tableName: String): TestConnectionInfo =
    val dr = new SQLServerDriver()
    val con = dr.connect(connectionUrl, new Properties())
    val query = "IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'arcane') BEGIN CREATE DATABASE arcane; alter database Arcane set CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON); END;"
    val statement = con.createStatement()
    statement.execute(query)
    createTable(tableName, con)
    util.TestConnectionInfo(
      ConnectionOptions(
        connectionUrl,
        "arcane",
        "dbo",
        tableName,
        Some("format(getdate(), 'yyyyMM')")), con)

  def createTable(tableName: String, con: Connection): Unit =
    val query = s"use arcane; drop table if exists dbo.$tableName; create table dbo.$tableName (x int not null, y int)"
    val statement = con.createStatement()
    statement.executeUpdate(query)

    val createPKCmd = s"use arcane; alter table dbo.$tableName add constraint pk_$tableName primary key(x);"
    statement.executeUpdate(createPKCmd)

    val enableCtCmd = s"use arcane; alter table dbo.$tableName enable change_tracking;"
    statement.executeUpdate(enableCtCmd)

  def insertData(con: Connection): Unit =
    val statement = con.createStatement()
    for i <- 1 to 10 do
      val insertCmd = s"use arcane; insert into dbo.MsSqlConnectorsTests values($i, ${i+1})"
      statement.execute(insertCmd)
    statement.close()

    val updateStatement = con.createStatement()
    for i <- 1 to 10 do
      val insertCmd = s"use arcane; insert into dbo.MsSqlConnectorsTests values(${i * 1000}, ${i * 1000 + 1})"
      updateStatement.execute(insertCmd)

  def deleteData(connection: Connection, primaryKeys: Seq[Int]): ZIO[Any, Throwable, Unit] = ZIO.scoped {
    for
      statement <- ZIO.attempt(connection.prepareStatement("DELETE FROM dbo.MsSqlConnectorsTests WHERE x = ?"))
      _ <- ZIO.foreachDiscard(primaryKeys) { number =>
        ZIO.attempt {
          statement.setInt(1, number)
          statement.executeUpdate()
        }
      }
    yield ()
  }

  def removeDb(): Unit =
    val query = "DROP DATABASE arcane"
    val dr = new SQLServerDriver()
    val con = dr.connect(connectionUrl, new Properties())
    val statement = con.createStatement()
    statement.execute(query)


  def withDatabase(test: TestConnectionInfo => Future[Assertion]): Future[Assertion] =
    val conn = createDb("MsSqlConnectorsTests")
    insertData(conn.connection)
    test(conn)

  def withFreshTable(tableName: String)(test: TestConnectionInfo => Future[Assertion]): Future[Assertion] =
    val conn = createDb(tableName)
    test(conn)

  "QueryProvider" should "generate columns query" in withDatabase { dbInfo =>
    val connector = MsSqlConnection(dbInfo.connectionOptions)
    val query = QueryProvider.getColumnSummariesQuery(connector.connectionOptions.schemaName,
      connector.connectionOptions.tableName,
      connector.connectionOptions.databaseName)
    query.get should include ("case when kcu.CONSTRAINT_NAME is not null then 1 else 0 end as IsPrimaryKey")
  }

  "QueryProvider" should "generate schema query" in withDatabase { dbInfo =>
    val connector = MsSqlConnection(dbInfo.connectionOptions)
    QueryProvider.getSchemaQuery(connector) map { query =>
      query should (
        include ("ct.SYS_CHANGE_VERSION") and include ("ARCANE_MERGE_KEY") and include("format(getdate(), 'yyyyMM')")
        )
    }
  }

  "QueryProvider" should "generate time-based query if previous version not provided" in withDatabase { dbInfo =>
    val connector = MsSqlConnection(dbInfo.connectionOptions)
    val formattedTime = constantFormatter.format(LocalDateTime.now().minus(Duration.ofHours(-1)))
    val query = QueryProvider.getChangeTrackingVersionQuery(None, Duration.ofHours(-1))
    query should (include ("SELECT MIN(commit_ts)") and include (s"WHERE commit_time > '$formattedTime'"))
  }
  
  "QueryProvider" should "generate version-based query if previous version is provided" in withDatabase { dbInfo =>
    val connector = MsSqlConnection(dbInfo.connectionOptions)
    val formattedTime = constantFormatter.format(LocalDateTime.now().minus(Duration.ofHours(-1)))
    val query = QueryProvider.getChangeTrackingVersionQuery(Some(1), Duration.ofHours(-1))
    query should (include ("SELECT MIN(commit_ts)") and (not include "commit_time") and include (s"WHERE commit_ts > 1"))
  }

  "QueryProvider" should "generate backfill query" in withDatabase { dbInfo =>
    val connector = MsSqlConnection(dbInfo.connectionOptions)
    QueryProvider.getBackfillQuery(connector) map { query =>
      query should (
        include ("SYS_CHANGE_VERSION") and include ("ARCANE_MERGE_KEY") and include("format(getdate(), 'yyyyMM')")
        )
    }
  }
  
  "MsSqlConnection" should "be able to extract schema column names from the database" in withDatabase { dbInfo =>
    val connection = MsSqlConnection(dbInfo.connectionOptions)
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(connection.getSchema)) map { schema =>
      val fields = for column <- schema if column.isInstanceOf[ArcaneSchemaField] yield column.name
      fields should be (List("x", "SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION", "y", "ChangeTrackingVersion", "ARCANE_MERGE_KEY", "DATE_PARTITION_KEY"))
    }
  }


  "MsSqlConnection" should "be able to extract schema column types from the database" in withDatabase { dbInfo =>
    val connection = MsSqlConnection(dbInfo.connectionOptions)
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(connection.getSchema)) map { schema =>
      val fields = for column <- schema if column.isInstanceOf[ArcaneSchemaField] yield column.fieldType
      fields should be(List(IntType, LongType, StringType, IntType, LongType, StringType, StringType))
    }
  }

  "MsSqlConnection" should "return correct number of rows on backfill" in withDatabase { dbInfo =>
    val connection = MsSqlConnection(dbInfo.connectionOptions)
    val future = Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(connection.getSchema))
    for schema <- future
        backfill <- connection.backfill
        result = backfill.read.toList
    yield {
      result should have length 20
    }
  }

  "MsSqlConnection" should "return correct number of columns on backfill" in withDatabase { dbInfo =>
    val connection = MsSqlConnection(dbInfo.connectionOptions)
    val future = Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(connection.getSchema))
    for schema <- future
        backfill <- connection.backfill
        result = backfill.read.toList
        head = result.head
    yield {
      head should have length 7
    }
  }

  "MsSqlConnection" should "return correct number of rows on getChanges" in withDatabase { dbInfo =>
    val connection = MsSqlConnection(dbInfo.connectionOptions)
    val future = Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(connection.getSchema))
    for schema <- future
        result <- connection.getChanges(None, Duration.ofDays(1))
        (columns, _ ) = result
        changedData = columns.read.toList
    yield {
      changedData should have length 20
    }
  }

  "MsSqlConnection" should "handle deletes" in withDatabase { dbInfo =>
    val connection = MsSqlConnection(dbInfo.connectionOptions)
    for schema <- Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(connection.getSchema))
        result <- connection.getChanges(None, Duration.ofDays(1))
        (columns, version) = result
        _ <- Future(columns.close())
        _ <- Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(deleteData(dbInfo.connection, Seq(2))))
        result2 <- connection.getChanges(Some(version), Duration.ofDays(1))
        (columns2, _) = result2
        changedData = columns2.read.toList
    yield {
      changedData(1) should contain allOf (
        DataCell("SYS_CHANGE_OPERATION", StringType, "D"),

        // NOTE: the value here is computed manually
        DataCell("ARCANE_MERGE_KEY", StringType, "913da1f8df6f8fd47593840d533ba0458cc9873996bf310460abb495b34c232a")
      )
    }
  }

  "MsSqlConnection" should "update latest version when changes received" in withDatabase { dbInfo =>
    val connection = MsSqlConnection(dbInfo.connectionOptions)
    val future = Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(connection.getSchema))
    for schema <- future
        result <- connection.getChanges(None, Duration.ofDays(1))
        (_, latestVersion) = result
    yield {
      latestVersion should be >= 0L
    }
  }

