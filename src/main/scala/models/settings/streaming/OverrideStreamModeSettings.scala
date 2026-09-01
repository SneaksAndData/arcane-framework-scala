package com.sneaksanddata.arcane.framework
package models.settings.streaming

import models.settings.backfill.{DefaultOverrideBackfillSettings, OverrideBackfillSettings}
import upickle.ReadWriter

/** A partial override of `StreamModeSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideStreamModeSettings:
  val backfill: Option[OverrideBackfillSettings]

case class DefaultOverrideStreamModeSettings(
    override val backfill: Option[DefaultOverrideBackfillSettings] = None
) extends OverrideStreamModeSettings derives ReadWriter
