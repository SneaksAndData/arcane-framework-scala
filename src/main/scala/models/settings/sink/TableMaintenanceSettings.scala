package com.sneaksanddata.arcane.framework
package models.settings.sink

import models.settings.Mergeable

import upickle.ReadWriter

/** The settings related to the maintenance of the table
  */
trait TableMaintenanceSettings extends Mergeable:

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
) extends TableMaintenanceSettings,
      Mergeable derives ReadWriter:

  override type MergeableFrom = OverrideTableMaintenanceSettings
  override type MergeResult   = DefaultTableMaintenanceSettings

  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    DefaultTableMaintenanceSettings(
      targetOptimizeSettings = this.targetOptimizeSettings.merge(overrides.flatMap(_.targetOptimizeSettings)),
      targetSnapshotExpirationSettings =
        this.targetSnapshotExpirationSettings.merge(overrides.flatMap(_.targetSnapshotExpirationSettings)),
      targetOrphanFilesExpirationSettings =
        this.targetOrphanFilesExpirationSettings.merge(overrides.flatMap(_.targetOrphanFilesExpirationSettings)),
      targetAnalyzeSettings = this.targetAnalyzeSettings.merge(overrides.flatMap(_.targetAnalyzeSettings))
    )
