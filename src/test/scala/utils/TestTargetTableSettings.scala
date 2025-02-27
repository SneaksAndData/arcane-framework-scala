package com.sneaksanddata.arcane.framework
package utils

import models.settings.{OptimizeSettings, OrphanFilesExpirationSettings, SnapshotExpirationSettings, TargetTableSettings}

object TestTargetTableSettings extends TargetTableSettings:
  override val targetTableFullName: String = "database.schema.target_table"
  override val targetOptimizeSettings: Option[OptimizeSettings] = None
  override val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings] = None
  override val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings] = None