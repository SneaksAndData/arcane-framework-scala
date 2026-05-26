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
