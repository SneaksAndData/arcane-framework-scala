package com.sneaksanddata.arcane.framework
package services.streaming.throughput.base

import models.app.PluginStreamContext
import models.settings.sink.SinkSettings
import models.settings.streaming.ThroughputSettings
import models.settings.streaming.ThroughputShaperImpl.{MemoryBound, Static}
import models.settings.TableNaming.*
import services.iceberg.base.SinkPropertyManager
import services.metrics.DeclaredMetrics
import services.streaming.throughput.{MemoryBoundShaper, StaticShaper}

import zio.{ZIO, ZLayer}

/** Factory class for ThroughputShaper implementations
  */
class ThroughputShaperBuilder(
    throughputSettings: ThroughputSettings,
    propertyManager: SinkPropertyManager,
    targetTableShortName: String,
    declaredMetrics: DeclaredMetrics
):

  def build: ThroughputShaper = throughputSettings.shaperImpl match
    case Static => StaticShaper(throughputSettings)
    case mb: MemoryBound =>
      MemoryBoundShaper(propertyManager, targetTableShortName, throughputSettings, declaredMetrics)

object ThroughputShaperBuilder:
  def apply(
      throughputSettings: ThroughputSettings,
      propertyManager: SinkPropertyManager,
      targetTableShortName: String,
      declaredMetrics: DeclaredMetrics
  ): ThroughputShaperBuilder =
    new ThroughputShaperBuilder(throughputSettings, propertyManager, targetTableShortName, declaredMetrics)

  val layer = ZLayer {
    for
      context         <- ZIO.service[PluginStreamContext]
      propertyManager <- ZIO.service[SinkPropertyManager]
      metrics         <- ZIO.service[DeclaredMetrics]
    yield ThroughputShaperBuilder(context.throughput, propertyManager, context.sink.targetTableFullName.parts.name, metrics)
  }
