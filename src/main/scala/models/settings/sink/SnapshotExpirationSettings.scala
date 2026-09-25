package com.sneaksanddata.arcane.framework
package models.settings.sink

import models.settings.Mergeable

import upickle.ReadWriter

/** Settings for snapshot expiration
  */
trait SnapshotExpirationSettings:

  /** Number of batches to trigger snapshot expiration
    */
  val batchThreshold: Int

  /** Retention threshold for snapshot expiration
    */
  val retentionThreshold: String

case class DefaultSnapshotExpirationSettings(
    override val retentionThreshold: String,
    override val batchThreshold: Int
) extends SnapshotExpirationSettings,
      Mergeable derives ReadWriter:

  override type MergeableFrom = OverrideSnapshotExpirationSettings
  override type MergeResult   = DefaultSnapshotExpirationSettings

  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    DefaultSnapshotExpirationSettings(
      retentionThreshold = overrides.flatMap(_.retentionThreshold).getOrElse(this.retentionThreshold),
      batchThreshold = overrides.flatMap(_.batchThreshold).getOrElse(this.batchThreshold)
    )
