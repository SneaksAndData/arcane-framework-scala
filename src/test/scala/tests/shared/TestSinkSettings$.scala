package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.{IcebergSinkSettings, SinkSettings, TableMaintenanceSettings}

object TestSinkSettings$ extends SinkSettings:
  override val targetTableFullName: String                   = "database.schema.target_table"
  override val maintenanceSettings: TableMaintenanceSettings = EmptyTestTableMaintenanceSettings
  override val icebergSinkSettings: IcebergSinkSettings      = IcebergCatalogInfo.defaultSinkSettings

object TestSinkSettingsWithMaintenance$ extends SinkSettings:
  override val targetTableFullName: String                   = "database.schema.target_table"
  override val maintenanceSettings: TableMaintenanceSettings = TestTableMaintenanceSettings
  override val icebergSinkSettings: IcebergSinkSettings      = IcebergCatalogInfo.defaultSinkSettings

class TestDynamicSinkSettings(name: String) extends SinkSettings:
  override val targetTableFullName: String                   = name
  override val maintenanceSettings: TableMaintenanceSettings = EmptyTestTableMaintenanceSettings
  override val icebergSinkSettings: IcebergSinkSettings      = IcebergCatalogInfo.defaultSinkSettings
