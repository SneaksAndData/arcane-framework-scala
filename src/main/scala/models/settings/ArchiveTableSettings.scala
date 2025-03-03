package com.sneaksanddata.arcane.framework
package models.settings

/**
 * Settings for the archive table
 */
trait ArchiveTableSettings:
  
  /**
   * Full name of the archive table
   */
  val fullName: String

  val maintenanceSettings: TableMaintenanceSettings
