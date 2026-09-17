package com.sneaksanddata.arcane.framework
package models.settings.sink

import models.settings.{DefaultOverrideTablePropertiesSettings, OverrideTablePropertiesSettings}
import models.settings.iceberg.DefaultOverrideIcebergCatalogSettings
import models.settings.staging.DefaultOverrideJdbcMergeServiceClientSettings

import upickle.ReadWriter

/** A partial override of `SinkSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideSinkSettings:
  /** Optional override for the target table name.
    */
  val targetTableFullName: Option[String]

  /** Optional override for the table maintenance settings.
    */
  val maintenanceSettings: Option[DefaultOverrideTableMaintenanceSettings]

  /** Optional override for the Iceberg catalog configuration.
    */
  val icebergCatalog: Option[DefaultOverrideIcebergCatalogSettings]

  /** Optional override for the merge service client settings.
    */
  val mergeServiceClient: Option[DefaultOverrideJdbcMergeServiceClientSettings]

  /** Optional override for the target table properties.
    */
  val targetTableProperties: Option[DefaultOverrideTablePropertiesSettings]

/** Default implementation for `OverrideSinkSettings` using optional values.
  */
case class DefaultOverrideSinkSettings(
    override val targetTableFullName: Option[String] = None,
    override val maintenanceSettings: Option[DefaultOverrideTableMaintenanceSettings] = None,
    override val icebergCatalog: Option[DefaultOverrideIcebergCatalogSettings] = None,
    override val mergeServiceClient: Option[DefaultOverrideJdbcMergeServiceClientSettings] = None,
    override val targetTableProperties: Option[DefaultOverrideTablePropertiesSettings] = None
) extends OverrideSinkSettings derives ReadWriter
