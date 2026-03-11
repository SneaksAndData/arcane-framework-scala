package com.sneaksanddata.arcane.framework
package models.settings.sink

import models.settings.staging.{DefaultJdbcMergeServiceClientSettings, JdbcMergeServiceClientSettings}
import models.settings.{DefaultTablePropertiesSettings, TablePropertiesSettings}
import models.settings.TableName

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
  val icebergCatalog: IcebergSinkSettings

  /** Merge client configuration
    */
  val mergeServiceClient: JdbcMergeServiceClientSettings

  /**
   * Additional properties for table creation: partitions, sort order etc.
   */
  val targetTableProperties: TablePropertiesSettings


case class DefaultSinkSettings(
    override val icebergCatalog: DefaultIcebergSinkSettings,
    override val maintenanceSettings: DefaultTableMaintenanceSettings,
    override val targetTableFullName: String,
    override val targetTableProperties: DefaultTablePropertiesSettings,
    override val mergeServiceClient: DefaultJdbcMergeServiceClientSettings
) extends SinkSettings derives ReadWriter
