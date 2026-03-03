package com.sneaksanddata.arcane.framework
package models.settings.staging

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.UUID

/** Settings for staging data
  */
trait StagingDataSettings:

  /** The prefix for the staging table name
    */
  val stagingTablePrefix: String

  /** The name of the catalog where the staging table is located
    */
  val stagingCatalogName: String

  /** The name of the schema where the staging table is located
    */
  val stagingSchemaName: String

  /** Indicates that all batches have the same schema. This setting should be hard-coded in the plugin and not exposed
    * in the Stream Spec
    */
  val isUnifiedSchema: Boolean

  /** Creates a name for the staging table
    */
  def newStagingTableName: String =
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss")
    val id                           = UUID.randomUUID().toString
    s"${stagingTablePrefix}__${ZonedDateTime.now(ZoneOffset.UTC).format(formatter)}_$id".replace('-', '_')
