package com.sneaksanddata.arcane.framework
package models.settings

import java.time.{ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.UUID

trait StagingDataSettings:
  val stagingTablePrefix: String
  
  val stagingCatalogName: String
  
  val stagingSchemaName: String

  /**
   * Indicates that all batches have the same schema.
   * This setting should be hard-coded in the plugin and not exposed in the Stream Spec
   */
  val isUnifiedSchema: Boolean
  
  def newStagingTableName: String =
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss")
    val id = UUID.randomUUID().toString
    s"${stagingTablePrefix}__${ZonedDateTime.now(ZoneOffset.UTC).format(formatter)}_$id".replace('-', '_')
