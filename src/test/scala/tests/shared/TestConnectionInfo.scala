package com.sneaksanddata.arcane.framework
package tests.shared

import com.sneaksanddata.arcane.framework.services.mssql.base.ConnectionOptions
import java.sql.Connection

case class TestConnectionInfo(connectionOptions: ConnectionOptions, connection: Connection)
