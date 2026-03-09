package com.sneaksanddata.arcane.framework
package models.settings.sink

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
) extends OrphanFilesExpirationSettings derives ReadWriter
