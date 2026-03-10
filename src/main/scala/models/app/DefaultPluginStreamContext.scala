package com.sneaksanddata.arcane.framework
package models.app

import models.settings.observability.DefaultObservabilitySettings
import models.settings.sink.DefaultSinkSettings
import models.settings.staging.DefaultStagingSettings
import models.settings.streaming.{DefaultStreamModeSettings, DefaultThroughputSettings}

abstract class DefaultPluginStreamContext(
    final override val observability: DefaultObservabilitySettings,
    final override val staging: DefaultStagingSettings,
    final override val streamMode: DefaultStreamModeSettings,
    final override val sink: DefaultSinkSettings,
    final override val throughput: DefaultThroughputSettings
) extends PluginStreamContext:

  override def merge(other: Option[PluginStreamContext]): PluginStreamContext = ???
