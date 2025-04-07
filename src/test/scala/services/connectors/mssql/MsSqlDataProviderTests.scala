package com.sneaksanddata.arcane.framework
package services.connectors.mssql

import models.app.StreamContext
import models.settings.VersionedDataGraphBuilderSettings
import services.connectors.mssql.util.TestConnectionInfo
import services.mssql.{ConnectionOptions, MsSqlConnection, MsSqlDataProvider, MsSqlStreamingDataProvider}
import utils.TestStreamLifetimeService

import com.microsoft.sqlserver.jdbc.SQLServerDriver
import org.scalatest.*
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.*
import zio.{Runtime, Unsafe}

import java.sql.Connection
import java.time.Duration
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.List
import scala.concurrent.Future
import scala.language.postfixOps

class MsSqlDataProviderTests extends flatspec.AsyncFlatSpec with Matchers:
  private val runtime = Runtime.default
  
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  
  /// To avoid mocking current date/time  we use the formatter that will always return the same value
  private implicit val constantFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("111")

  private val settings = new VersionedDataGraphBuilderSettings {
    override val lookBackInterval: Duration = Duration.ofHours(1)
    override val changeCaptureInterval: Duration = Duration.ofMillis(1)
    override val changeCapturePeriod: Duration = Duration.ofHours(1)
  }

  private val streamContext = new StreamContext:
    override val IsBackfilling = false

  val connectionUrl = "jdbc:sqlserver://localhost;encrypt=true;trustServerCertificate=true;username=sa;password=tMIxN11yGZgMC;databaseName=arcane"

  def createDb(tableName: String): TestConnectionInfo =
    val dr = new SQLServerDriver()
    val con = dr.connect(connectionUrl, new Properties())
    createTable(tableName, con)
    util.TestConnectionInfo(
      ConnectionOptions(
        connectionUrl,
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

  def insertData(con: Connection, tableName: String): Unit =
    val statement = con.createStatement()
    for i <- 1 to 10 do
      val insertCmd = s"use arcane; insert into $tableName values($i, ${i+1})"
      statement.execute(insertCmd)
    statement.close()

    val updateStatement = con.createStatement()
    for i <- 1 to 10 do
      val insertCmd = s"use arcane; insert into $tableName values(${i * 1000}, ${i * 1000 + 1})"
      updateStatement.execute(insertCmd)


  def removeDb(): Unit =
    val query = "DROP DATABASE arcane"
    val dr = new SQLServerDriver()
    val con = dr.connect(connectionUrl, new Properties())
    val statement = con.createStatement()
    statement.execute(query)


  def withDatabase(test: TestConnectionInfo => Future[Assertion]): Future[Assertion] =
    val tableName = "MsSqlDataProviderTests".toLowerCase()
    val conn = createDb(tableName)
    insertData(conn.connection, tableName)
    test(conn)

  def withFreshTable(tableName: String)(test: TestConnectionInfo => Future[Assertion]): Future[Assertion] =
    val conn = createDb(tableName)
    test(conn)

  it should "return correct number of rows while streaming" in withDatabase { dbInfo =>
    val numberRowsToTake = 5
    val connection = MsSqlConnection(dbInfo.connectionOptions)
    val dataProvider = MsSqlDataProvider(connection)
    val streamingDataProvider = MsSqlStreamingDataProvider(dataProvider, settings, streamContext)
    val lifetimeService = TestStreamLifetimeService(numberRowsToTake)

    val stream = streamingDataProvider.stream.takeWhile(_ => !lifetimeService.cancelled).runCollect

    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { rows =>
      rows should have size numberRowsToTake
    }
  }
