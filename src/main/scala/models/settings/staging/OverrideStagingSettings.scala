package com.sneaksanddata.arcane.framework
package models.settings.staging

import models.settings.iceberg.DefaultOverrideIcebergCatalogSettings

import upickle.ReadWriter

/** A partial override of `StagingSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideStagingSettings:
  /** Optional override for staging table settings.
    */
  val table: Option[DefaultOverrideStagingTableSettings]

  /** Optional override for the Iceberg catalog settings used for staging.
    */
  val icebergCatalog: Option[DefaultOverrideIcebergCatalogSettings]

/** Default implementation for `OverrideStagingSettings` using optional values.
  */
case class DefaultOverrideStagingSettings(
    override val table: Option[DefaultOverrideStagingTableSettings] = None,
    override val icebergCatalog: Option[DefaultOverrideIcebergCatalogSettings] = None
) extends OverrideStagingSettings derives ReadWriter
