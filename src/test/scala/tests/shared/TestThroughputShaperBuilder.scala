package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.FlowRate
import models.settings.TableNaming.*
import models.settings.sink.SinkSettings
import models.settings.streaming.{MemoryBound, MemoryBoundImpl, ThroughputSettings, ThroughputShaperImpl}
import services.iceberg.base.SinkPropertyManager
import services.metrics.DeclaredMetrics
import services.streaming.throughput.base.ThroughputShaperBuilder

import java.time.Duration

object TestThroughputShaperBuilder:
  def default(propertyManager: SinkPropertyManager, sinkSettings: SinkSettings): ThroughputShaperBuilder =
    ThroughputShaperBuilder(
      new ThroughputSettings {
        override val shaperImpl: ThroughputShaperImpl =
          MemoryBoundImpl(MemoryBound(1000, 4096, 1, 10, 0.5, 0.5, 2))
        override val advisedChunkSize: Int = 10
        override val advisedRate: FlowRate = FlowRate(elements = 1, interval = Duration.ofSeconds(10))
        override val advisedBurst: Int     = 10
      },
      propertyManager,
      sinkSettings.targetTableFullName.parts.name,
      DeclaredMetrics()
    )
