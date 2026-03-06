package com.sneaksanddata.arcane.framework
package services.streaming.throughput.base

import models.settings.sink.SinkSettings
import models.settings.streaming.ThroughputSettings
import models.settings.streaming.ThroughputShaperImpl.{MemoryBound, Static}
import services.iceberg.base.SinkPropertyManager
import services.metrics.DeclaredMetrics
import services.streaming.throughput.{MemoryBoundShaper, StaticShaper}

import zio.{ZIO, ZLayer}

/** Factory class for ThroughputShaper implementations
  * @param throughputSettings
  * @param propertyManager
  * @param sinkSettings
  */
class ThroughputShaperBuilder(
    throughputSettings: ThroughputSettings,
    propertyManager: SinkPropertyManager,
    sinkSettings: SinkSettings,
    declaredMetrics: DeclaredMetrics
):

  def build: ThroughputShaper = throughputSettings.shaperImpl match
    case Static          => StaticShaper(throughputSettings)
    case mb: MemoryBound => MemoryBoundShaper(propertyManager, sinkSettings, throughputSettings, declaredMetrics)

object ThroughputShaperBuilder:
  def apply(
      throughputSettings: ThroughputSettings,
      propertyManager: SinkPropertyManager,
      sinkSettings: SinkSettings,
      declaredMetrics: DeclaredMetrics
  ): ThroughputShaperBuilder =
    new ThroughputShaperBuilder(throughputSettings, propertyManager, sinkSettings, declaredMetrics)

  val layer = ZLayer {
    for
      settings        <- ZIO.service[ThroughputSettings]
      propertyManager <- ZIO.service[SinkPropertyManager]
      sinkSettings    <- ZIO.service[SinkSettings]
      metrics         <- ZIO.service[DeclaredMetrics]
    yield ThroughputShaperBuilder(settings, propertyManager, sinkSettings, metrics)
  }
