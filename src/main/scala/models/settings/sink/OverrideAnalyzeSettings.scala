package com.sneaksanddata.arcane.framework
package models.settings.sink

import upickle.ReadWriter

/** A partial override of `AnalyzeSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideAnalyzeSettings:
  /** Optional override for the threshold of batches that trigger analyze.
    */
  val batchThreshold: Option[Int]

  /** Optional override for the columns included in the analyze operation.
    */
  val includedColumns: Option[Seq[String]]

/** Default implementation for `OverrideAnalyzeSettings` using optional values.
  */
case class DefaultOverrideAnalyzeSettings(
    override val batchThreshold: Option[Int] = None,
    override val includedColumns: Option[Seq[String]] = None
) extends OverrideAnalyzeSettings derives ReadWriter
