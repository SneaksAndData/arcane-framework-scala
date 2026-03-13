package com.sneaksanddata.arcane.framework
package models.settings.mssql

import models.settings.sources.SourceSettings

trait DatabaseSourceSettings extends SourceSettings:
  /** JDBC Url to use when connecting to a database
    */
  val connectionUrl: String

  /** Database schema to use when interacting with the source
    */
  val schemaName: String

  /** Source table
    */
  val tableName: String

  /** Extra JDBC connection parameters
    */
  val extraConnectionParameters: Map[String, String]
