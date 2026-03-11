package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.sink.{SinkSettings, TableMaintenanceSettings}
import models.settings.staging.JdbcMergeServiceClientSettings
import models.settings.{EmptyTablePropertiesSettings, TablePropertiesSettings}
import models.settings.iceberg.IcebergCatalogSettings

object TestSinkSettings extends SinkSettings:
  override val targetTableFullName: String                        = "database.schema.target_table"
  override val maintenanceSettings: TableMaintenanceSettings      = EmptyTestTableMaintenanceSettings
  override val icebergCatalog: IcebergCatalogSettings                = IcebergCatalogInfo.defaultSinkSettings
  override val targetTableProperties: TablePropertiesSettings     = EmptyTablePropertiesSettings
  override val mergeServiceClient: JdbcMergeServiceClientSettings = TestJdbcMergeServiceClientSettings

object TestSinkSettingsWithMaintenance extends SinkSettings:
  override val targetTableFullName: String                        = "database.schema.target_table"
  override val maintenanceSettings: TableMaintenanceSettings      = TestTableMaintenanceSettings
  override val icebergCatalog: IcebergCatalogSettings = IcebergCatalogInfo.defaultSinkSettings
  override val targetTableProperties: TablePropertiesSettings     = EmptyTablePropertiesSettings
  override val mergeServiceClient: JdbcMergeServiceClientSettings = TestJdbcMergeServiceClientSettings

class TestDynamicSinkSettings(name: String) extends SinkSettings:
  override val targetTableFullName: String                        = name
  override val maintenanceSettings: TableMaintenanceSettings      = EmptyTestTableMaintenanceSettings
  override val icebergCatalog: IcebergCatalogSettings                = IcebergCatalogInfo.defaultSinkSettings
  override val targetTableProperties: TablePropertiesSettings     = EmptyTablePropertiesSettings
  override val mergeServiceClient: JdbcMergeServiceClientSettings = TestJdbcMergeServiceClientSettings
