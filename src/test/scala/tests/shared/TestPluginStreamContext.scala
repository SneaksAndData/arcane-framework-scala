package com.sneaksanddata.arcane.framework
package tests.shared

import models.app.PluginStreamContext
import models.settings.FieldSelectionRuleSettings
import models.settings.backfill.BackfillBehavior.Overwrite
import models.settings.backfill.{BackfillBehavior, BackfillSettings}
import models.settings.observability.ObservabilitySettings
import models.settings.sink.SinkSettings
import models.settings.sources.BufferingStrategy.Unbounded
import models.settings.sources.{BufferingStrategy, SourceBufferingSettings, SourceSettings, StreamSourceSettings}
import models.settings.staging.StagingSettings
import models.settings.streaming.ThroughputShaperImpl.Static
import models.settings.streaming.{ChangeCaptureSettings, StreamModeSettings, ThroughputSettings, ThroughputShaperImpl}

import java.time.{Duration, OffsetDateTime}

case object TestPluginStreamContext extends PluginStreamContext:
  override def isBackfilling: Boolean = false

  override def streamId: String = "test-stream-id"

  override def streamKind: String = "test-stream-kind"

  override val observability: ObservabilitySettings = TestObservabilitySettings
  override val streamMode: StreamModeSettings = new StreamModeSettings {
    override val backfill: BackfillSettings = new BackfillSettings {
      override val backfillTableFullName: String             = "catalog.schema.table"
      override val backfillStartDate: Option[OffsetDateTime] = None
      override val backfillBehavior: BackfillBehavior        = Overwrite
    }
    override val changeCapture: ChangeCaptureSettings = new ChangeCaptureSettings {
      override val changeCaptureInterval: Duration     = Duration.ofSeconds(10)
      override val changeCaptureJitterVariance: Double = 0.1
      override val changeCaptureJitterSeed: Long       = 1
    }
  }
  override val sink: SinkSettings = TestSinkSettings
  override val throughput: ThroughputSettings = new ThroughputSettings {
    override val shaperImpl: ThroughputShaperImpl = Static
    override val advisedChunkSize: Int            = 1
    override val advisedRateChunks: Int           = 1
    override val advisedRatePeriod: Duration      = Duration.ofSeconds(1)
    override val advisedChunksBurst: Int          = 1
  }
  override val staging: StagingSettings = TestStagingSettings()

  override def merge(other: Option[PluginStreamContext]): PluginStreamContext = ???

  override val source: StreamSourceSettings = new StreamSourceSettings {
    override type SourceSettingsType = SourceSettings
    override val source: SourceSettings = new SourceSettings {}
    override val buffering: SourceBufferingSettings = new SourceBufferingSettings {
      override val bufferingStrategy: BufferingStrategy = Unbounded
      override val bufferingEnabled: Boolean            = false
    }
    override val fieldSelectionRule: FieldSelectionRuleSettings = TestFieldSelectionRuleSettings
  }
