package com.sneaksanddata.arcane.framework
package migrations.base

import models.app.PluginStreamContext
import models.settings.TableNaming.parts
import zio.{Task, ZIO}

import java.sql.DriverManager

object MigrationQueryHelper:

  /** Checks if any rows in the target table have a NULL value in the specified column.
    *
    * @param targetTable
    *   Full name of the target table (catalog.schema.table)
    * @param columnName
    *   Name of the column to inspect
    * @param context
    *   Plugin stream context providing merge client connection settings
    * @return
    *   True if at least one row has NULL in the column, false otherwise
    */
  def hasNulls(
      targetTable: String,
      columnName: String,
      context: PluginStreamContext
  ): Task[Boolean] =
    val options = context.sink.mergeServiceClient
    val catalog = targetTable.parts.warehouse
    val schema  = targetTable.parts.namespace
    ZIO.scoped {
      for
        connection <- ZIO.fromAutoCloseable(
          ZIO.attemptBlocking(
            DriverManager.getConnection(options.getConnectionString(catalog, schema, options.credentialType))
          )
        )
        statement <- ZIO.fromAutoCloseable(
          ZIO.attempt(connection.prepareStatement(s"SELECT 1 FROM $targetTable WHERE $columnName IS NULL LIMIT 1"))
        )
        hasResult <- ZIO.attemptBlocking {
          val rs = statement.executeQuery()
          rs.next()
        }
      yield hasResult
    }
