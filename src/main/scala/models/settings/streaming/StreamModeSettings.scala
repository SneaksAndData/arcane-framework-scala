package com.sneaksanddata.arcane.framework
package models.settings.streaming

import models.settings.backfill.BackfillSettings

import upickle.ReadWriter

/**
 * Settings for available streaming mode
 */
trait StreamModeSettings:
  /**
   * Backfill mode-only settings
   */
  val backfill: BackfillSettings

  /**
   * Change capture mode settings
   */
  val changeCapture: ChangeCaptureSettings

case class DefaultStreamModeSettings(
                                      override val changeCapture: ChangeCaptureSettings,
                                      override val backfill: BackfillSettings
                                    ) extends StreamModeSettings derives ReadWriter