package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.sink.{IcebergSinkSettings, SinkSettings, TableMaintenanceSettings}
import models.settings.{EmptyTablePropertiesSettings, TablePropertiesSettings}

object TestSinkSettings extends SinkSettings:
  override val targetTableFullName: String                    = "database.schema.target_table"
  override val maintenanceSettings: TableMaintenanceSettings  = EmptyTestTableMaintenanceSettings
  override val icebergSinkSettings: IcebergSinkSettings       = IcebergCatalogInfo.defaultSinkSettings
  override val targetTableProperties: TablePropertiesSettings = EmptyTablePropertiesSettings

object TestSinkSettingsWithMaintenance extends SinkSettings:
  override val targetTableFullName: String                    = "database.schema.target_table"
  override val maintenanceSettings: TableMaintenanceSettings  = TestTableMaintenanceSettings
  override val icebergSinkSettings: IcebergSinkSettings       = IcebergCatalogInfo.defaultSinkSettings
  override val targetTableProperties: TablePropertiesSettings = EmptyTablePropertiesSettings

class TestDynamicSinkSettings(name: String) extends SinkSettings:
  override val targetTableFullName: String                    = name
  override val maintenanceSettings: TableMaintenanceSettings  = EmptyTestTableMaintenanceSettings
  override val icebergSinkSettings: IcebergSinkSettings       = IcebergCatalogInfo.defaultSinkSettings
  override val targetTableProperties: TablePropertiesSettings = EmptyTablePropertiesSettings
