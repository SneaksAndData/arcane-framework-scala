package com.sneaksanddata.arcane.framework
package models.settings.sink

import upickle.ReadWriter

/** The settings related to the maintenance of the table
  */
trait TableMaintenanceSettings:

  /** Optimization settings for the target table
    */
  val targetOptimizeSettings: Option[OptimizeSettings]

  /** Snapshot expiration settings for the target table
    */
  val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings]

  /** Orphan files expiration settings for the target table
    */
  val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings]

  /** Settings for running ANALYZE
    */
  val targetAnalyzeSettings: Option[AnalyzeSettings]

case class DefaultTableMaintenanceSettings(
    override val targetAnalyzeSettings: Option[DefaultAnalyzeSettings],
    override val targetOptimizeSettings: Option[DefaultOptimizeSettings],
    override val targetSnapshotExpirationSettings: Option[DefaultSnapshotExpirationSettings],
    override val targetOrphanFilesExpirationSettings: Option[DefaultOrphanFilesExpirationSettings]
) extends TableMaintenanceSettings derives ReadWriter
