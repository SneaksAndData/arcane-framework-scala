package com.sneaksanddata.arcane.framework
package models.settings.staging

import upickle.ReadWriter

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.UUID

/** Settings for staging data
  */
trait StagingTableSettings:

  /** The name of the catalog where the staging table is located
    */
  val stagingCatalogName: String

  /** The name of the schema where the staging table is located
    */
  val stagingSchemaName: String

  /** Indicates that all batches have the same schema.
    */
  val isUnifiedSchema: Boolean

  /** Max rows per file in each staging table
    */
  val maxRowsPerFile: Option[Int]

case class DefaultStagingTableSettings(
    override val maxRowsPerFile: Option[Int],
    override val stagingCatalogName: String,
    override val stagingSchemaName: String,
    override val isUnifiedSchema: Boolean
) extends StagingTableSettings derives ReadWriter
