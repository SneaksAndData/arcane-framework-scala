package com.sneaksanddata.arcane.framework
package tests.shared

import services.mssql.ConnectionOptions

import java.sql.Connection

case class TestConnectionInfo(connectionOptions: ConnectionOptions, connection: Connection)
