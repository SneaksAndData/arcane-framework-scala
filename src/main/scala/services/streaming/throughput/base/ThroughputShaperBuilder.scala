package com.sneaksanddata.arcane.framework
package services.streaming.throughput.base

import models.settings.sink.SinkSettings
import models.settings.streaming.ThroughputSettings
import models.settings.streaming.ThroughputShaperImpl.{MemoryBound, Static}
import services.iceberg.base.TablePropertyManager
import services.streaming.throughput.{MemoryBoundShaper, VoidShaper}

class ThroughputShaperBuilder(
    throughputSettings: ThroughputSettings,
    propertyManager: TablePropertyManager,
    sinkSettings: SinkSettings
):

  def build: ThroughputShaper = throughputSettings.shaperImpl match
    case Static      => VoidShaper()
    case MemoryBound => MemoryBoundShaper(propertyManager, sinkSettings, throughputSettings.shaperSettings)
