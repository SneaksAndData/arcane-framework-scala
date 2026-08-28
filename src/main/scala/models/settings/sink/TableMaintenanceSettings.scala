package com.sneaksanddata.arcane.framework
package models.settings.sink

import com.sneaksanddata.arcane.framework.models.settings.Mergeable
import upickle.ReadWriter

/** The settings related to the maintenance of the table
  */
trait TableMaintenanceSettings:

  /** Optimization settings for the target table
    */
  val targetOptimizeSettings: OptimizeSettings

  /** Snapshot expiration settings for the target table
    */
  val targetSnapshotExpirationSettings: SnapshotExpirationSettings

  /** Orphan files expiration settings for the target table
    */
  val targetOrphanFilesExpirationSettings: OrphanFilesExpirationSettings

  /** Settings for running ANALYZE
    */
  val targetAnalyzeSettings: AnalyzeSettings

case class DefaultTableMaintenanceSettings(
    override val targetAnalyzeSettings: DefaultAnalyzeSettings,
    override val targetOptimizeSettings: DefaultOptimizeSettings,
    override val targetSnapshotExpirationSettings: DefaultSnapshotExpirationSettings,
    override val targetOrphanFilesExpirationSettings: DefaultOrphanFilesExpirationSettings
) extends TableMaintenanceSettings, Mergeable[DefaultTableMaintenanceSettings] derives ReadWriter:
  override def merge(base: DefaultTableMaintenanceSettings, overrides: DefaultTableMaintenanceSettings): DefaultTableMaintenanceSettings =
    DefaultTableMaintenanceSettings(
      targetAnalyzeSettings = base.targetAnalyzeSettings.merge(base.targetAnalyzeSettings, overrides.targetAnalyzeSettings),
      targetOptimizeSettings = base.targetOptimizeSettings.merge(base.targetOptimizeSettings, overrides.targetOptimizeSettings),
      targetSnapshotExpirationSettings = base.targetSnapshotExpirationSettings.merge(base.targetSnapshotExpirationSettings, overrides.targetSnapshotExpirationSettings),
      targetOrphanFilesExpirationSettings = base.targetOrphanFilesExpirationSettings.merge(base.targetOrphanFilesExpirationSettings, overrides.targetOrphanFilesExpirationSettings)
    )
