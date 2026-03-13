package com.sneaksanddata.arcane.framework
package models.settings.mssql

import models.settings.database.DatabaseSourceSettings
import models.settings.database.JdbcConnectionExtensions.*

import upickle.ReadWriter

/** Microsoft SQL Server database connection settings
  */
trait MsSqlServerDatabaseSourceSettings extends DatabaseSourceSettings:
  final def getConnectionString: String = connectionUrl.withParameters(extraConnectionParameters)

  /** Fetch size for ResultSets.
    */
  val fetchSize: Option[Int]

case class DefaultMsSqlServerDatabaseSourceSettings(
    override val extraConnectionParameters: Map[String, String],
    override val connectionUrl: String,
    override val schemaName: String,
    override val tableName: String,
    override val fetchSize: Option[Int]
) extends MsSqlServerDatabaseSourceSettings derives ReadWriter
