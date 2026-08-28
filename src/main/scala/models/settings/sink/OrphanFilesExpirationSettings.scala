package com.sneaksanddata.arcane.framework
package models.settings.sink

import com.sneaksanddata.arcane.framework.models.settings.Mergeable
import upickle.ReadWriter

/** Settings for orphan files expiration
  */
trait OrphanFilesExpirationSettings:

  /** Number of batches to trigger orphan files expiration
    */
  val batchThreshold: Int

  /** Retention threshold for orphan files expiration
    */
  val retentionThreshold: String

case class DefaultOrphanFilesExpirationSettings(
    override val retentionThreshold: String,
    override val batchThreshold: Int
) extends OrphanFilesExpirationSettings,
      Mergeable[DefaultOrphanFilesExpirationSettings] derives ReadWriter:
  override def merge(
      base: DefaultOrphanFilesExpirationSettings,
      overrides: DefaultOrphanFilesExpirationSettings
  ): DefaultOrphanFilesExpirationSettings =
    DefaultOrphanFilesExpirationSettings(
      retentionThreshold = overrides.retentionThreshold,
      batchThreshold = overrides.batchThreshold
    )
