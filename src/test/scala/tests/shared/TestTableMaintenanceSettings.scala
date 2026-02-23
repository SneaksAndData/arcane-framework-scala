package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.sink.*

object TestTableMaintenanceSettings extends TableMaintenanceSettings:
  override val targetOptimizeSettings: Option[OptimizeSettings] = Some(TestOptimizeSettings)
  override val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings] = Some(
    TestSnapshotExpirationSettings
  )
  override val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings] = Some(
    TestOrphanFilesExpirationSettings
  )
  override val targetAnalyzeSettings: Option[AnalyzeSettings] = Some(TestAnalyzeSettings)

object EmptyTestTableMaintenanceSettings extends TableMaintenanceSettings:
  override val targetOptimizeSettings: Option[OptimizeSettings]                           = None
  override val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings]       = None
  override val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings] = None
  override val targetAnalyzeSettings: Option[AnalyzeSettings]                             = None
