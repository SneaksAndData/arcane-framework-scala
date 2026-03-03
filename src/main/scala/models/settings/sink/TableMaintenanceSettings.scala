package com.sneaksanddata.arcane.framework
package models.settings.sink

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
