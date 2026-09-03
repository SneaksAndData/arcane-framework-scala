package com.sneaksanddata.arcane.framework
package models.settings.sink

import models.settings.{TableName, TablePropertiesSettings}
import models.settings.iceberg.IcebergCatalogSettings
import models.settings.staging.JdbcMergeServiceClientSettings

import upickle.ReadWriter

/** A partial override of `SinkSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideSinkSettings:
  /** Optional override for the fully qualified target table name.
    */
  val targetTableFullName: Option[TableName]

  /** Optional override for maintenance settings applied to the target table.
    */
  val maintenanceSettings: Option[TableMaintenanceSettings]

  /** Optional override for the Iceberg catalog settings used by the sink.
    */
  val icebergCatalog: Option[IcebergCatalogSettings]

  /** Optional override for the JDBC merge client settings.
    */
  val mergeServiceClient: Option[JdbcMergeServiceClientSettings]

  /** Optional override for table properties used during table creation and maintenance.
    */
  val targetTableProperties: Option[TablePropertiesSettings]

/** Default implementation for `OverrideSinkSettings` using optional values.
  */
case class DefaultOverrideSinkSettings(
    override val targetTableFullName: Option[TableName] = None,
    override val maintenanceSettings: Option[TableMaintenanceSettings] = None,
    override val icebergCatalog: Option[IcebergCatalogSettings] = None,
    override val mergeServiceClient: Option[JdbcMergeServiceClientSettings] = None,
    override val targetTableProperties: Option[TablePropertiesSettings] = None
) extends OverrideSinkSettings derives ReadWriter
