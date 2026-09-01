package com.sneaksanddata.arcane.framework
package models.settings.streaming

import models.settings.Mergeable
import models.settings.backfill.{BackfillSettings, DefaultBackfillSettings}

import upickle.ReadWriter

/** Settings for available streaming mode
  */
trait StreamModeSettings extends Mergeable:

  /** Backfill mode-only settings
    */
  val backfill: BackfillSettings

  /** Change capture mode settings
    */
  val changeCapture: ChangeCaptureSettings

case class DefaultStreamModeSettings(
    override val changeCapture: DefaultChangeCaptureSettings,
    override val backfill: DefaultBackfillSettings
) extends StreamModeSettings derives ReadWriter:

  override type MergeableFrom = OverrideStreamModeSettings
  override type MergeResult = DefaultStreamModeSettings
  override def merge(overrides: MergeableFrom): MergeResult =
    DefaultStreamModeSettings(
      changeCapture = this.changeCapture,
      backfill = overrides.backfill.map(b => this.backfill.merge(b)).getOrElse(this.backfill)
    )
