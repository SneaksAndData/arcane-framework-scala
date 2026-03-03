package com.sneaksanddata.arcane.framework
package services.streaming.throughput.base

import models.settings.sink.SinkSettings
import models.settings.streaming.ThroughputSettings
import models.settings.streaming.ThroughputShaperImpl.{MemoryBound, Static}
import services.iceberg.base.TablePropertyManager
import services.streaming.throughput.{MemoryBoundShaper, StaticShaper}

import zio.{ZIO, ZLayer}

/** Factory class for ThroughputShaper implementations
  * @param throughputSettings
  * @param propertyManager
  * @param sinkSettings
  */
class ThroughputShaperBuilder(
    throughputSettings: ThroughputSettings,
    propertyManager: TablePropertyManager,
    sinkSettings: SinkSettings
):

  def build: ThroughputShaper = throughputSettings.shaperImpl match
    case Static          => StaticShaper(throughputSettings)
    case mb: MemoryBound => MemoryBoundShaper(propertyManager, sinkSettings, throughputSettings)

object ThroughputShaperBuilder:
  def apply(
      throughputSettings: ThroughputSettings,
      propertyManager: TablePropertyManager,
      sinkSettings: SinkSettings
  ): ThroughputShaperBuilder = new ThroughputShaperBuilder(throughputSettings, propertyManager, sinkSettings)

  val layer = ZLayer {
    for
      settings        <- ZIO.service[ThroughputSettings]
      propertyManager <- ZIO.service[TablePropertyManager]
      sinkSettings    <- ZIO.service[SinkSettings]
    yield ThroughputShaperBuilder(settings, propertyManager, sinkSettings)
  }
