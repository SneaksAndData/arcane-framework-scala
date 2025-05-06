package com.sneaksanddata.arcane.framework
package tests.services.connectors.mssql.util

import services.mssql.ConnectionOptions

import java.sql.Connection

case class TestConnectionInfo(connectionOptions: ConnectionOptions, connection: Connection)
