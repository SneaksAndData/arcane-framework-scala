package com.sneaksanddata.arcane.framework
package models.settings.staging

import models.settings.Mergeable
import models.settings.iceberg.{DefaultIcebergStagingSettings, IcebergCatalogSettings, OverrideIcebergCatalogSettings}

import upickle.ReadWriter

/** Staging configuration
  */
trait StagingSettings extends Mergeable:
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
      table = this.table.merge(overrides.flatMap(_.table)),
      icebergCatalog = this.icebergCatalog.merge(overrides.flatMap(_.icebergCatalog))
    )
