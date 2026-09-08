package com.sneaksanddata.arcane.framework
package models.settings.streaming

import models.settings.backfill.DefaultOverrideBackfillSettings

import upickle.ReadWriter

/** A partial override of `StreamModeSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideStreamModeSettings:
  /** Optional override for the backfill-mode settings.
    */
  val backfill: Option[DefaultOverrideBackfillSettings]

  /** Optional override for the change-capture settings.
    */
  val changeCapture: Option[DefaultOverrideChangeCaptureSettings]

/** Default implementation for `OverrideStreamModeSettings` using optional values.
  */
case class DefaultOverrideStreamModeSettings(
    override val backfill: Option[DefaultOverrideBackfillSettings] = None,
    override val changeCapture: Option[DefaultOverrideChangeCaptureSettings] = None
) extends OverrideStreamModeSettings derives ReadWriter
