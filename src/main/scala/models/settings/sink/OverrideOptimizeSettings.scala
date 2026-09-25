package com.sneaksanddata.arcane.framework
package models.settings.sink

import upickle.ReadWriter

/** A partial override of `OptimizeSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideOptimizeSettings:
  /** Optional override for the threshold of batches that trigger optimization.
    */
  val batchThreshold: Option[Int]

  /** Optional override for the file size threshold that triggers optimization.
    */
  val fileSizeThreshold: Option[String]

/** Default implementation for `OverrideOptimizeSettings` using optional values.
  */
case class DefaultOverrideOptimizeSettings(
    override val batchThreshold: Option[Int] = None,
    override val fileSizeThreshold: Option[String] = None
) extends OverrideOptimizeSettings derives ReadWriter
