package com.sneaksanddata.arcane.framework
package models.settings.sink

import upickle.ReadWriter

/** A partial override of `OrphanFilesExpirationSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideOrphanFilesExpirationSettings:
  /** Optional override for the threshold of batches that trigger orphan file expiration.
    */
  val batchThreshold: Option[Int]

  /** Optional override for the retention threshold used for orphan file expiration.
    */
  val retentionThreshold: Option[String]

/** Default implementation for `OverrideOrphanFilesExpirationSettings` using optional values.
  */
case class DefaultOverrideOrphanFilesExpirationSettings(
    override val batchThreshold: Option[Int] = None,
    override val retentionThreshold: Option[String] = None
) extends OverrideOrphanFilesExpirationSettings derives ReadWriter
