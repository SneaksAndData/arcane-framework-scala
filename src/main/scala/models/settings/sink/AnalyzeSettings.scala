package com.sneaksanddata.arcane.framework
package models.settings.sink

import upickle.ReadWriter

/** Settings for orphan files expiration
  */
trait AnalyzeSettings:

  /** Number of batches to trigger orphan files expiration
    */
  val batchThreshold: Int

  /** Optional columns to limit ANALYZE to
    */
  val includedColumns: Seq[String]

case class DefaultAnalyzeSettings(
                                   override val includedColumns: Seq[String],
                                   override val batchThreshold: Int
                                 ) extends AnalyzeSettings derives ReadWriter