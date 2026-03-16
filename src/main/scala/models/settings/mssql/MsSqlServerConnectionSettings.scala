package com.sneaksanddata.arcane.framework
package models.settings.mssql

import models.settings.database.DatabaseSourceSettings
import models.settings.database.JdbcConnectionExtensions.*

import upickle.ReadWriter
import upickle.implicits.key

/** Microsoft SQL Server database connection settings
  */
trait MsSqlServerDatabaseSourceSettings extends DatabaseSourceSettings:
  final def getConnectionString: String = connectionUrl.withConnectionParameters(extraConnectionParameters)

  /** Fetch size for ResultSets.
    */
  val fetchSize: Option[Int]

case class DefaultMsSqlServerDatabaseSourceSettings(
    override val extraConnectionParameters: Map[String, String],
    @key("connectionUrl") connectionString: Option[String] = None,
    override val schemaName: String,
    override val tableName: String,
    override val fetchSize: Option[Int]
) extends MsSqlServerDatabaseSourceSettings derives ReadWriter:
  override val connectionUrl: String =
    connectionString.getOrElse(sys.env("ARCANE_FRAMEWORK__MICROSOFT_SQL_SERVER_CONNECTION_URI"))
