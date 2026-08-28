package com.sneaksanddata.arcane.framework
package models.settings.staging

import models.settings.iceberg.{DefaultIcebergStagingSettings, IcebergCatalogSettings}

import com.sneaksanddata.arcane.framework.models.settings.Mergeable
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
      Mergeable[DefaultStagingSettings] derives ReadWriter:
  override def merge(base: DefaultStagingSettings, overrides: DefaultStagingSettings): DefaultStagingSettings =
    DefaultStagingSettings(
      table = base.table.merge(base.table, overrides.table),
      icebergCatalog = base.icebergCatalog.merge(base.icebergCatalog, overrides.icebergCatalog)
    )
