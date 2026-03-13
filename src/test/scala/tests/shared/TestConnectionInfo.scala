package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.mssql.MsSqlServerDatabaseSourceSettings

import java.sql.Connection

case class TestConnectionInfo(connectionOptions: MsSqlServerDatabaseSourceSettings, connection: Connection)
