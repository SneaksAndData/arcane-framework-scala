package com.sneaksanddata.arcane.framework
package models.settings

/**
 * Settings for the archive table
 */
trait ArchiveTableSettings:
  
  /**
   * Full name of the archive table
   */
  val fullName: String
  
  /**
   * Optimize settings for the archive table
   */
  val optimizeSettings: OptimizeSettings
  
  /**
   * Snapshot expiration settings for the archive table
   */
  val snapshotExpirationSettings: SnapshotExpirationSettings
  
  /**
   * Orphan files expiration settings for the archive table
   */
  val orphanFileExpirationSettings: OrphanFilesExpirationSettings
