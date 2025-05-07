package com.sneaksanddata.arcane.framework
package models.settings

/** Settings for snapshot expiration
  */
trait SnapshotExpirationSettings:

  /** Number of batches to trigger snapshot expiration
    */
  val batchThreshold: Int

  /** Retention threshold for snapshot expiration
    */
  val retentionThreshold: String
