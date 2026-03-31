package com.sneaksanddata.arcane.framework
package tests.shared

import models.app.PluginStreamContext
import models.settings.FieldSelectionRuleSettings
import models.settings.backfill.BackfillBehavior.{Merge, Overwrite}
import models.settings.backfill.{BackfillBehavior, BackfillSettings}
import models.settings.observability.ObservabilitySettings
import models.settings.sink.SinkSettings
import models.settings.sources.{
  BufferingStrategy,
  SourceBufferingSettings,
  SourceSettings,
  StreamSourceSettings,
  Unbounded,
  UnboundedImpl
}
import models.settings.staging.StagingSettings
import models.settings.streaming.{
  ChangeCaptureSettings,
  Static,
  StaticImpl,
  StreamModeSettings,
  ThroughputSettings,
  ThroughputShaperImpl
}

import zio.{IO, ZIO}

import java.time.{Duration, OffsetDateTime}

abstract class TestPluginStreamContextImpl extends PluginStreamContext:
  override def isBackfilling: ZIO[Any, SecurityException, Boolean] = ZIO.succeed(false)

  override def streamId: IO[SecurityException, String] = ZIO.succeed("test-stream-id")

  override def streamKind: IO[SecurityException, String] = ZIO.succeed("test-stream-kind")

  override val observability: ObservabilitySettings = TestObservabilitySettings
  override val streamMode: StreamModeSettings = new StreamModeSettings {
    override val backfill: BackfillSettings = new BackfillSettings {
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
    override val shaperImpl: ThroughputShaperImpl = StaticImpl(Static())
    override val advisedChunkSize: Int            = 1
    override val advisedRateChunks: Int           = 1
    override val advisedRatePeriod: Duration      = Duration.ofSeconds(1)
    override val advisedChunksBurst: Int          = 1
  }
  override val staging: StagingSettings = TestStagingSettings()

  override def merge(other: Option[PluginStreamContext]): PluginStreamContext = ???

  override val source: StreamSourceSettings = new StreamSourceSettings {
    override type SourceSettingsType = SourceSettings
    override val configuration: SourceSettings = new SourceSettings {}
    override val buffering: SourceBufferingSettings = new SourceBufferingSettings {
      override val bufferingStrategy: BufferingStrategy = UnboundedImpl(Unbounded())
      override val bufferingEnabled: Boolean            = false
    }
    override val fieldSelectionRule: FieldSelectionRuleSettings = TestFieldSelectionRuleSettings
  }

object TestPluginStreamContext extends TestPluginStreamContextImpl:
  override def isBackfilling: ZIO[Any, SecurityException, Boolean] = ZIO.succeed(false)

object TestPluginBackfillOverwriteStreamContext extends TestPluginStreamContextImpl:
  override def isBackfilling: ZIO[Any, SecurityException, Boolean] = ZIO.succeed(true)

object TestPluginBackfillMergeStreamContext extends TestPluginStreamContextImpl:
  override def isBackfilling: ZIO[Any, SecurityException, Boolean] = ZIO.succeed(true)

  override val streamMode: StreamModeSettings = new StreamModeSettings {
    override val backfill: BackfillSettings = new BackfillSettings {
      override val backfillStartDate: Option[OffsetDateTime] = None
      override val backfillBehavior: BackfillBehavior        = Merge
    }
    override val changeCapture: ChangeCaptureSettings = new ChangeCaptureSettings {
      override val changeCaptureInterval: Duration     = Duration.ofSeconds(10)
      override val changeCaptureJitterVariance: Double = 0.1
      override val changeCaptureJitterSeed: Long       = 1
    }
  }
