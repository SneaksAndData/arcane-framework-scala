package com.sneaksanddata.arcane.framework
package models.settings.sink

import models.settings.iceberg.IcebergCatalogSettings
import models.settings.staging.{DefaultJdbcMergeServiceClientSettings, JdbcMergeServiceClientSettings}
import models.settings.{DefaultTablePropertiesSettings, Mergeable, TableName, TablePropertiesSettings}

import upickle.ReadWriter

/** Settings for the target table
  */
trait SinkSettings:
  /** The name of the target table
    */
  val targetTableFullName: TableName

  /** The maintenance settings for the target table
    */
  val maintenanceSettings: TableMaintenanceSettings

  /** Settings for Iceberg Catalog instance associated with the sink
    */
  val icebergCatalog: IcebergCatalogSettings

  /** Merge client configuration
    */
  val mergeServiceClient: JdbcMergeServiceClientSettings

  /** Additional properties for table creation: partitions, sort order etc.
    */
  val targetTableProperties: TablePropertiesSettings

case class DefaultSinkSettings(
    override val icebergCatalog: DefaultIcebergSinkSettings,
    override val maintenanceSettings: DefaultTableMaintenanceSettings,
    override val targetTableFullName: String,
    override val targetTableProperties: DefaultTablePropertiesSettings,
    override val mergeServiceClient: DefaultJdbcMergeServiceClientSettings
) extends SinkSettings, Mergeable[DefaultSinkSettings] derives ReadWriter:
  override def merge(base: DefaultSinkSettings, overrides: DefaultSinkSettings): DefaultSinkSettings =
    DefaultSinkSettings(
      icebergCatalog = base.icebergCatalog.merge(base.icebergCatalog, overrides.icebergCatalog),
      maintenanceSettings = base.maintenanceSettings.merge(base.maintenanceSettings, overrides.maintenanceSettings),
      targetTableFullName = overrides.targetTableFullName,
      targetTableProperties = base.targetTableProperties.merge(base.targetTableProperties, overrides.targetTableProperties),
      mergeServiceClient = base.mergeServiceClient.merge(base.mergeServiceClient, overrides.mergeServiceClient)
    )
