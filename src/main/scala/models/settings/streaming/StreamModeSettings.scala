package com.sneaksanddata.arcane.framework
package models.settings.streaming

import models.settings.backfill.{BackfillSettings, DefaultBackfillSettings}

import com.sneaksanddata.arcane.framework.models.settings.Mergeable
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
) extends StreamModeSettings, Mergeable[DefaultStreamModeSettings] derives ReadWriter:
  override def merge(base: DefaultStreamModeSettings, overrides: DefaultStreamModeSettings): DefaultStreamModeSettings =
    DefaultStreamModeSettings(
      changeCapture = base.changeCapture.merge(base.changeCapture, overrides.changeCapture),
      backfill = base.backfill.merge(base.backfill, overrides.backfill)
    )
