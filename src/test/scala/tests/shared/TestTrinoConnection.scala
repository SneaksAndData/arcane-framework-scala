package com.sneaksanddata.arcane.framework
package tests.shared

import io.trino.jdbc.TrinoDriver
import zio.{Task, ZIO}

import java.sql.Connection
import java.util.Properties

object TestTrinoConnection:
  def newTrinoConnection: Task[Connection] =
    for
      driver     <- ZIO.succeed(new TrinoDriver())
      connection <- ZIO.attemptBlocking(driver.connect("jdbc:trino://localhost:8080?user=test", new Properties()))
    yield connection

  def getRowsInTarget(con: Connection, tableName: String): ZIO[Any, Throwable, Int] = ZIO.scoped {
    ZIO
      .fromAutoCloseable(ZIO.attempt(con.prepareStatement(s"select count (1) from $tableName")))
      .map { st =>
        val rs = st.executeQuery()
        if rs.next() then rs.getInt(1)
        else 0
      }
  }
  
  def getFieldValueInTarget(con: Connection, tableName: String, fieldName: String, pkKey: String, pkValue: String): ZIO[Any, Throwable, String] = ZIO.scoped {
    ZIO
      .fromAutoCloseable(ZIO.attempt(con.prepareStatement(s"select $fieldName from $tableName where $pkKey = '$pkValue'")))
      .map { st =>
        val rs = st.executeQuery()
        if rs.next() then rs.getString(1)
        else ""
      }
  }
