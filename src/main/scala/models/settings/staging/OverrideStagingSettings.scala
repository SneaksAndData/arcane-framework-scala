package com.sneaksanddata.arcane.framework
package models.settings.staging

import models.settings.iceberg.IcebergCatalogSettings

import upickle.ReadWriter

/** A partial override of `StagingSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideStagingSettings:
  /** Optional override for staging table configuration.
    */
  val table: Option[StagingTableSettings]

  /** Optional override for the Iceberg catalog settings used by staging.
    */
  val icebergCatalog: Option[IcebergCatalogSettings]

/** Default implementation for `OverrideStagingSettings` using optional values.
  */
case class DefaultOverrideStagingSettings(
    override val table: Option[StagingTableSettings] = None,
    override val icebergCatalog: Option[IcebergCatalogSettings] = None
) extends OverrideStagingSettings derives ReadWriter
