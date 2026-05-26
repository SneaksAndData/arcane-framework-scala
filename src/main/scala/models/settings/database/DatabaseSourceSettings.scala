package com.sneaksanddata.arcane.framework
package models.settings.database

import models.settings.sources.SourceSettings

type JdbcConnectionUrl = String

trait DatabaseSourceSettings extends SourceSettings:
  /** JDBC Url to use when connecting to a database
    */
  val connectionUrl: JdbcConnectionUrl

  /** Database schema to use when interacting with the source
    */
  val schemaName: String

  /**
   * Database schema to use for storing backfill shard tables
   */
  val backfillShardSchemaName: String

  /** Source table
    */
  val tableName: String

  /** Extra JDBC connection parameters
    */
  val extraConnectionParameters: Map[String, String]
