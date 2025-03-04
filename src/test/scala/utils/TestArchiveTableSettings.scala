package com.sneaksanddata.arcane.framework
package utils

import models.settings.TableMaintenanceSettings

object TestArchiveTableSettings extends ArchiveTableSettings:
  override val fullName: String = "database.schema.archive_table"
  override val maintenanceSettings: TableMaintenanceSettings = TestTableMaintenanceSettings 


