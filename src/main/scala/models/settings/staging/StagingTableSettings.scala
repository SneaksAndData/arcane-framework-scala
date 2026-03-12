package com.sneaksanddata.arcane.framework
package models.settings.staging

import upickle.ReadWriter

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.UUID

/** Settings for staging data
  */
trait StagingTableSettings:

  /** The prefix for the staging table name
    */
  val stagingTablePrefix: String

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

  /** Creates a name for the staging table
    */
  def newStagingTableName: String =
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss")
    val id                           = UUID.randomUUID().toString
    s"${stagingTablePrefix}__${ZonedDateTime.now(ZoneOffset.UTC).format(formatter)}_$id".replace('-', '_')

  /** Name for the backfill table to be used in the current run.
    */
  final val backfillTableName: String =
    s"$stagingCatalogName.$stagingSchemaName.${stagingTablePrefix}__backfill_${UUID.randomUUID().toString}"
      .replace("-", "_")

case class DefaultStagingTableSettings(
    override val stagingTablePrefix: String,
    override val maxRowsPerFile: Option[Int],
    override val stagingCatalogName: String,
    override val stagingSchemaName: String,
    override val isUnifiedSchema: Boolean
) extends StagingTableSettings derives ReadWriter
