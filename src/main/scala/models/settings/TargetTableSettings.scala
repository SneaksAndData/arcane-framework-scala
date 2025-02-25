package com.sneaksanddata.arcane.framework
package models.settings

trait TargetTableSettings:
  val targetTableFullName: String
  val targetOptimizeSettings: Option[OptimizeSettings]
  val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings]
  val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings]
