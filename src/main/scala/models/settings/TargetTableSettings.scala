package com.sneaksanddata.arcane.framework
package models.settings

trait TargetTableSettings:
  val targetTableFullName: String
  val maintenanceSettings: TableMaintenanceSettings