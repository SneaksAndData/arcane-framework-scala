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
) extends SinkSettings,
      Mergeable derives ReadWriter:

  override type MergeableFrom = OverrideSinkSettings
  override type MergeResult   = DefaultSinkSettings

  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    DefaultSinkSettings(
      icebergCatalog = overrides.flatMap(_.icebergCatalog).getOrElse(this.icebergCatalog),
      maintenanceSettings = overrides.flatMap(_.maintenanceSettings).getOrElse(this.maintenanceSettings),
      targetTableFullName = overrides.flatMap(_.targetTableFullName).getOrElse(this.targetTableFullName),
      targetTableProperties = overrides.flatMap(_.targetTableProperties).getOrElse(this.targetTableProperties),
      mergeServiceClient = overrides.flatMap(_.mergeServiceClient).getOrElse(this.mergeServiceClient)
    )
