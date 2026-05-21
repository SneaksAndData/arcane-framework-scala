package com.sneaksanddata.arcane.framework
package tests.synapse

import models.settings.backfill.BackfillBehavior.Overwrite
import models.settings.backfill.{BackfillBehavior, BackfillSettings}
import models.settings.streaming.{ChangeCaptureSettings, StreamModeSettings}

import java.time.{Duration, OffsetDateTime, ZoneOffset}

object SynapseLinkTestSettings:
  val defaultStreamMode: StreamModeSettings = new StreamModeSettings {

    /** Backfill mode-only settings
     */
    override val backfill: BackfillSettings = new BackfillSettings {
      override val backfillBehavior: BackfillBehavior = Overwrite
      override val backfillStartDate: Option[OffsetDateTime] = Some(
        OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofHours(12))
      )
    }

    /** Change capture mode settings
     */
    override val changeCapture: ChangeCaptureSettings = new ChangeCaptureSettings {
      override val changeCaptureInterval: Duration = Duration.ofSeconds(5)
      override val changeCaptureJitterVariance: Double = 0.0001
      override val changeCaptureJitterSeed: Long = 0
    }
  }
