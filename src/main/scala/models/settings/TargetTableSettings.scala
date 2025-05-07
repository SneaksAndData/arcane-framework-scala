package com.sneaksanddata.arcane.framework
package models.settings

/** Settings for the target table
  */
trait TargetTableSettings:
  /** The name of the target table
    */
  val targetTableFullName: String

  /** The maintenance settings for the target table
    */
  val maintenanceSettings: TableMaintenanceSettings
