package com.sneaksanddata.arcane.framework
package models.settings.sink

/** Settings for orphan files expiration
  */
trait AnalyzeSettings:

  /** Number of batches to trigger orphan files expiration
    */
  val batchThreshold: Int

  /** Optional columns to limit ANALYZE to
    */
  val includedColumns: Seq[String]
