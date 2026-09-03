package com.sneaksanddata.arcane.framework
package models.settings.streaming

import models.settings.Mergeable
import models.settings.backfill.{BackfillSettings, DefaultBackfillSettings}

import upickle.ReadWriter

/** Settings for available streaming mode
  */
trait StreamModeSettings:

  /** Backfill mode-only settings
    */
  val backfill: BackfillSettings

  /** Change capture mode settings
    */
  val changeCapture: ChangeCaptureSettings

case class DefaultStreamModeSettings(
    override val changeCapture: DefaultChangeCaptureSettings,
    override val backfill: DefaultBackfillSettings
) extends StreamModeSettings,
      Mergeable derives ReadWriter:

  override type MergeableFrom = OverrideStreamModeSettings
  override type MergeResult   = DefaultStreamModeSettings
  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    DefaultStreamModeSettings(
      changeCapture = this.changeCapture.merge(overrides.flatMap(_.changeCapture)),
      backfill = this.backfill.merge(overrides.flatMap(_.backfill))
    )
