package com.sneaksanddata.arcane.framework
package models.settings

import java.time.{ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.UUID

/**
 * Represents the settings for staging data.
 */
trait StagingDataSettings:

  /**
   * The prefix for the staging table.
   */
  val stagingTablePrefix: String

  /**
   * The name of the staging catalog.
   */
  val stagingCatalogName: String

  /**
   * The name of the staging schema.
   */
  val stagingSchemaName: String


  /**
   * The name of the staging table. For now, it's just a prefix.
   */
  def getStagingTableName: String = stagingTablePrefix
