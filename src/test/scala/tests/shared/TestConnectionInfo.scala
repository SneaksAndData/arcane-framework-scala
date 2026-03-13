package com.sneaksanddata.arcane.framework
package tests.shared

import com.sneaksanddata.arcane.framework.models.settings.mssql.MsSqlServerConnectionSettings
import java.sql.Connection

case class TestConnectionInfo(connectionOptions: MsSqlServerConnectionSettings, connection: Connection)
