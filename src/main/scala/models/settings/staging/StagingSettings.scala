package com.sneaksanddata.arcane.framework
package models.settings.staging

import models.settings.Mergeable
import models.settings.iceberg.{DefaultIcebergStagingSettings, IcebergCatalogSettings}

import upickle.ReadWriter

/** Staging configuration
  */
trait StagingSettings:
  /** Settings for staging tables management
    */
  val table: StagingTableSettings

  /** Iceberg REST Catalog configuration for staging tables
    */
  val icebergCatalog: IcebergCatalogSettings

case class DefaultStagingSettings(
    override val table: DefaultStagingTableSettings,
    override val icebergCatalog: DefaultIcebergStagingSettings
) extends StagingSettings,
      Mergeable derives ReadWriter:

  override type MergeableFrom = OverrideStagingSettings
  override type MergeResult   = DefaultStagingSettings

  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    DefaultStagingSettings(
      table = overrides.flatMap(_.table).getOrElse(this.table),
      icebergCatalog = overrides.flatMap(_.icebergCatalog).getOrElse(this.icebergCatalog)
    )
