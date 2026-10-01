package com.sneaksanddata.arcane.framework
package models.settings.sink

import models.settings.Mergeable

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
) extends OptimizeSettings,
      Mergeable derives ReadWriter:

  override type MergeableFrom = OverrideOptimizeSettings
  override type MergeResult   = DefaultOptimizeSettings

  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    DefaultOptimizeSettings(
      fileSizeThreshold = overrides.flatMap(_.fileSizeThreshold).getOrElse(this.fileSizeThreshold),
      batchThreshold = overrides.flatMap(_.batchThreshold).getOrElse(this.batchThreshold)
    )
