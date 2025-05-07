package com.sneaksanddata.arcane.framework
package models.settings

import java.sql.DriverManager
import scala.util.Try

trait JdbcMergeServiceClientSettings:
  /** The connection URL.
    */
  val connectionUrl: String

  /** Checks if the connection URL is valid.
    *
    * @return
    *   True if the connection URL is valid, false otherwise.
    */
  final def isValid: Boolean = Try(DriverManager.getDriver(connectionUrl)).isSuccess
