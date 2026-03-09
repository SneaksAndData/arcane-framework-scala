package com.sneaksanddata.arcane.framework
package models.settings.sink

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
) extends SnapshotExpirationSettings derives ReadWriter
