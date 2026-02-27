package com.sneaksanddata.arcane.framework
package models.settings.sink

/** Settings for orphan files expiration
  */
trait OrphanFilesExpirationSettings:

  /** Number of batches to trigger orphan files expiration
    */
  val batchThreshold: Int

  /** Retention threshold for orphan files expiration
    */
  val retentionThreshold: String
