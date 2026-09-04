package com.sneaksanddata.arcane.framework
package models.settings.sink

import upickle.ReadWriter

/** A partial override of `TableMaintenanceSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideTableMaintenanceSettings:
  /** Optional override for the optimize settings.
    */
  val targetOptimizeSettings: Option[DefaultOverrideOptimizeSettings]

  /** Optional override for the snapshot expiration settings.
    */
  val targetSnapshotExpirationSettings: Option[DefaultOverrideSnapshotExpirationSettings]

  /** Optional override for the orphan files expiration settings.
    */
  val targetOrphanFilesExpirationSettings: Option[DefaultOverrideOrphanFilesExpirationSettings]

  /** Optional override for the ANALYZE settings.
    */
  val targetAnalyzeSettings: Option[DefaultOverrideAnalyzeSettings]

/** Default implementation for `OverrideTableMaintenanceSettings` using optional values.
  */
case class DefaultOverrideTableMaintenanceSettings(
    override val targetOptimizeSettings: Option[DefaultOverrideOptimizeSettings] = None,
    override val targetSnapshotExpirationSettings: Option[DefaultOverrideSnapshotExpirationSettings] = None,
    override val targetOrphanFilesExpirationSettings: Option[DefaultOverrideOrphanFilesExpirationSettings] = None,
    override val targetAnalyzeSettings: Option[DefaultOverrideAnalyzeSettings] = None
) extends OverrideTableMaintenanceSettings derives ReadWriter
