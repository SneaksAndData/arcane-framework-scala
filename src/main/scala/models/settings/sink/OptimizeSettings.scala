package com.sneaksanddata.arcane.framework
package models.settings.sink

import upickle.ReadWriter

/** Settings for optimizing the data table
  */
trait OptimizeSettings:

  /** Number of batches to trigger optimization
    */
  val batchThreshold: Int

  /** Optimize when the file size exceeds this threshold
    */
  val fileSizeThreshold: String

case class DefaultOptimizeSettings(
    override val fileSizeThreshold: String,
    override val batchThreshold: Int
) extends OptimizeSettings derives ReadWriter
