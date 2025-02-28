package com.sneaksanddata.arcane.framework
package models.settings

trait TableMaintenanceSettings:
  val targetOptimizeSettings: Option[OptimizeSettings]
  val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings]
  val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings]
