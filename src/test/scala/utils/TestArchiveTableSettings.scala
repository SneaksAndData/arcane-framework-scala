package com.sneaksanddata.arcane.framework
package utils

import models.settings.{ArchiveTableSettings, OptimizeSettings, OrphanFilesExpirationSettings, SnapshotExpirationSettings}

object TestArchiveTableSettings extends ArchiveTableSettings:
  override val fullName: String = "database.schema.archive_table"
  override val optimizeSettings: OptimizeSettings = new OptimizeSettings:
    /**
     * Number of batches to trigger optimization
     */
    override val batchThreshold: Int = 10
    /**
     * Optimize when the file size exceeds this threshold
     */
    override val fileSizeThreshold: String = "1GB"

  override val snapshotExpirationSettings: SnapshotExpirationSettings = new SnapshotExpirationSettings:
    /**
     * Number of batches to trigger snapshot expiration
     */
    override val batchThreshold: Int = 10
    /**
     * Retention threshold for snapshot expiration
     */
    override val retentionThreshold: String = "6h"

  override val orphanFileExpirationSettings: OrphanFilesExpirationSettings = new OrphanFilesExpirationSettings:
    /**
     * Number of batches to trigger orphan files expiration
     */
    override val batchThreshold: Int = 10
    /**
     * Retention threshold for orphan files expiration
     */
    override val retentionThreshold: String = "6h"


