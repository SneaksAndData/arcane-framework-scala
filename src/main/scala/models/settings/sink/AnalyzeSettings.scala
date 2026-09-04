package com.sneaksanddata.arcane.framework
package models.settings.sink

import models.settings.Mergeable

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
) extends AnalyzeSettings,
      Mergeable derives ReadWriter:

  override type MergeableFrom = OverrideAnalyzeSettings
  override type MergeResult   = DefaultAnalyzeSettings

  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    DefaultAnalyzeSettings(
      includedColumns = overrides.flatMap(_.includedColumns).getOrElse(this.includedColumns),
      batchThreshold = overrides.flatMap(_.batchThreshold).getOrElse(this.batchThreshold)
    )
