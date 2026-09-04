package com.sneaksanddata.arcane.framework
package models.settings.sink

import models.settings.Mergeable

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
      Mergeable derives ReadWriter:

  override type MergeableFrom = OverrideOrphanFilesExpirationSettings
  override type MergeResult   = DefaultOrphanFilesExpirationSettings

  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    DefaultOrphanFilesExpirationSettings(
      retentionThreshold = overrides.flatMap(_.retentionThreshold).getOrElse(this.retentionThreshold),
      batchThreshold = overrides.flatMap(_.batchThreshold).getOrElse(this.batchThreshold)
    )
