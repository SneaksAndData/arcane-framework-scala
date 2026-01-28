package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.{TableMaintenanceSettings, SinkSettings}

object TestSinkSettings$ extends SinkSettings:
  override val targetTableFullName: String                   = "database.schema.target_table"
  override val maintenanceSettings: TableMaintenanceSettings = EmptyTestTableMaintenanceSettings

object TestSinkSettingsWithMaintenance$ extends SinkSettings:
  override val targetTableFullName: String                   = "database.schema.target_table"
  override val maintenanceSettings: TableMaintenanceSettings = TestTableMaintenanceSettings

class TestDynamicSinkSettings(name: String) extends SinkSettings:
  override val targetTableFullName: String                   = name
  override val maintenanceSettings: TableMaintenanceSettings = EmptyTestTableMaintenanceSettings
