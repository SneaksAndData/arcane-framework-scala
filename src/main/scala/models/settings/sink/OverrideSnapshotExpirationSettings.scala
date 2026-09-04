package com.sneaksanddata.arcane.framework
package models.settings.sink

import upickle.ReadWriter

/** A partial override of `SnapshotExpirationSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideSnapshotExpirationSettings:
  /** Optional override for the threshold of batches that trigger snapshot expiration.
    */
  val batchThreshold: Option[Int]

  /** Optional override for the retention threshold used for snapshot expiration.
    */
  val retentionThreshold: Option[String]

/** Default implementation for `OverrideSnapshotExpirationSettings` using optional values.
  */
case class DefaultOverrideSnapshotExpirationSettings(
    override val batchThreshold: Option[Int] = None,
    override val retentionThreshold: Option[String] = None
) extends OverrideSnapshotExpirationSettings derives ReadWriter
