package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.{
  OptimizeSettings,
  OrphanFilesExpirationSettings,
  SnapshotExpirationSettings,
  TableMaintenanceSettings
}

object TestTableMaintenanceSettings extends TableMaintenanceSettings:
  override val targetOptimizeSettings: Option[OptimizeSettings] = Some(TestOptimizeSettings)
  override val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings] = Some(
    TestSnapshotExpirationSettings
  )
  override val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings] = Some(
    TestOrphanFilesExpirationSettings
  )

object EmptyTestTableMaintenanceSettings extends TableMaintenanceSettings:
  override val targetOptimizeSettings: Option[OptimizeSettings]                           = None
  override val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings]       = None
  override val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings] = None
