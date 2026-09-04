package com.sneaksanddata.arcane.framework
package models.settings.staging

import models.settings.Mergeable

import upickle.ReadWriter

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.UUID

/** Settings for staging data
  */
trait StagingTableSettings extends Mergeable:

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
) extends StagingTableSettings,
      Mergeable derives ReadWriter:

  override type MergeableFrom = OverrideStagingTableSettings
  override type MergeResult   = DefaultStagingTableSettings

  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    DefaultStagingTableSettings(
      maxRowsPerFile = overrides.flatMap(_.maxRowsPerFile).orElse(this.maxRowsPerFile),
      stagingCatalogName = overrides.flatMap(_.stagingCatalogName).getOrElse(this.stagingCatalogName),
      stagingSchemaName = overrides.flatMap(_.stagingSchemaName).getOrElse(this.stagingSchemaName),
      isUnifiedSchema = overrides.flatMap(_.isUnifiedSchema).getOrElse(this.isUnifiedSchema)
    )
