package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.sink.*

object TestTableMaintenanceSettings extends TableMaintenanceSettings:
  override val targetOptimizeSettings: OptimizeSettings                           = TestOptimizeSettings
  override val targetSnapshotExpirationSettings: SnapshotExpirationSettings       = TestSnapshotExpirationSettings
  override val targetOrphanFilesExpirationSettings: OrphanFilesExpirationSettings = TestOrphanFilesExpirationSettings
  override val targetAnalyzeSettings: AnalyzeSettings                             = TestAnalyzeSettings
