package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.{TableMaintenanceSettings, TargetTableSettings}

object TestTargetTableSettings extends TargetTableSettings:
  override val targetTableFullName: String                   = "database.schema.target_table"
  override val maintenanceSettings: TableMaintenanceSettings = EmptyTestTableMaintenanceSettings

object TestTargetTableSettingsWithMaintenance extends TargetTableSettings:
  override val targetTableFullName: String                   = "database.schema.target_table"
  override val maintenanceSettings: TableMaintenanceSettings = TestTableMaintenanceSettings

class TestDynamicTargetTableSettings(name: String) extends TargetTableSettings:
  override val targetTableFullName: String                   = name
  override val maintenanceSettings: TableMaintenanceSettings = EmptyTestTableMaintenanceSettings
