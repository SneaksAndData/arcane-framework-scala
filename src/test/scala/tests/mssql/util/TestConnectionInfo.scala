package com.sneaksanddata.arcane.framework
package tests.mssql.util

import services.mssql.base.ConnectionOptions

import com.microsoft.sqlserver.jdbc.SQLServerDriver
import zio.{Task, ZIO}

import java.sql.Connection
import java.time.format.DateTimeFormatter
import java.util.Properties

case class TestConnectionInfo(connectionOptions: ConnectionOptions, connection: Connection)

object MsSqlTestServices:
  val connectionUrl =
    "jdbc:sqlserver://localhost:1433;encrypt=true;trustServerCertificate=true;username=sa;password=tMIxN11yGZgMC;databaseName=arcane"

  /// To avoid mocking current date/time  we use the formatter that will always return the same value
  implicit val constantFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("111")

  def getConnection: Task[Connection] = for
    dr  <- ZIO.attempt(new SQLServerDriver())
    con <- ZIO.attemptBlocking(dr.connect(connectionUrl, new Properties()))
  yield con

  def createTable(tableName: String, con: Connection, fieldString: String, pkString: String): Unit =
    val query     = s"use arcane; drop table if exists dbo.$tableName; create table dbo.$tableName $fieldString"
    val statement = con.createStatement()
    statement.executeUpdate(query)

    val createPKCmd = s"use arcane; alter table dbo.$tableName add constraint pk_$tableName $pkString;"
    statement.executeUpdate(createPKCmd)

    val enableCtCmd = s"use arcane; alter table dbo.$tableName enable change_tracking;"
    statement.executeUpdate(enableCtCmd)
