package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.sink.SinkSettings
import models.settings.streaming.{MemoryBound, MemoryBoundImpl, ThroughputSettings, ThroughputShaperImpl}
import models.settings.TableNaming.*
import services.iceberg.base.SinkPropertyManager
import services.metrics.DeclaredMetrics
import services.streaming.throughput.base.ThroughputShaperBuilder

import java.time.Duration

object TestThroughputShaperBuilder:
  def default(propertyManager: SinkPropertyManager, sinkSettings: SinkSettings): ThroughputShaperBuilder =
    ThroughputShaperBuilder(
      new ThroughputSettings {
        override val shaperImpl: ThroughputShaperImpl =
          MemoryBoundImpl(MemoryBound(1000, 4096, 2, 2, 1, 10, 0.5, 0.5, 2))
        override val advisedChunkSize: Int       = 10
        override val advisedRateChunks: Int      = 1
        override val advisedRatePeriod: Duration = Duration.ofSeconds(10)
        override val advisedChunksBurst: Int     = 10
      },
      propertyManager,
      sinkSettings.targetTableFullName.parts.name,
      DeclaredMetrics(NullDimensionsProvider)
    )
