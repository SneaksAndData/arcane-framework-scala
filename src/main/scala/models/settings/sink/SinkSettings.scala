package com.sneaksanddata.arcane.framework
package models.settings.sink

import models.settings.Mergeable
import models.settings.iceberg.{IcebergCatalogSettings, OverrideIcebergCatalogSettings}
import models.settings.staging.{
  DefaultJdbcMergeServiceClientSettings,
  JdbcMergeServiceClientSettings,
  OverrideJdbcMergeServiceClientSettings
}
import models.settings.{
  DefaultTablePropertiesSettings,
  TableName,
  TablePropertiesSettings,
  OverrideTablePropertiesSettings
}

import upickle.ReadWriter

/** Settings for the target table
  */
trait SinkSettings extends Mergeable:
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
) extends SinkSettings,
      Mergeable derives ReadWriter:

  override type MergeableFrom = OverrideSinkSettings
  override type MergeResult   = DefaultSinkSettings

  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    DefaultSinkSettings(
      icebergCatalog = this.icebergCatalog.merge(overrides.flatMap(_.icebergCatalog)),
      maintenanceSettings = this.maintenanceSettings.merge(overrides.flatMap(_.maintenanceSettings)),
      targetTableFullName = overrides.flatMap(_.targetTableFullName).getOrElse(this.targetTableFullName),
      targetTableProperties = this.targetTableProperties.merge(overrides.flatMap(_.targetTableProperties)),
      mergeServiceClient = this.mergeServiceClient.merge(overrides.flatMap(_.mergeServiceClient))
    )
