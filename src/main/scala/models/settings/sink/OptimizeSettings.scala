package com.sneaksanddata.arcane.framework
package models.settings.sink

import com.sneaksanddata.arcane.framework.models.settings.Mergeable
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
) extends OptimizeSettings, Mergeable[DefaultOptimizeSettings] derives ReadWriter:
  override def merge(base: DefaultOptimizeSettings, overrides: DefaultOptimizeSettings): DefaultOptimizeSettings =
    DefaultOptimizeSettings(
      fileSizeThreshold = overrides.fileSizeThreshold,
      batchThreshold = overrides.batchThreshold
    )
