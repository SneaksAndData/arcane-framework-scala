package com.sneaksanddata.arcane.framework
package models.app

import models.settings.observability.DefaultObservabilitySettings
import models.settings.sink.DefaultSinkSettings
import models.settings.staging.DefaultStagingSettings
import models.settings.streaming.{DefaultStreamModeSettings, DefaultThroughputSettings}

/** Baseline implementation of a `PluginStreamContext`. Provides default, serializable configuration sections for
  * settings that compose foundational framework components.
  *
  * Plugin developers must provide a concrete implementation of this class by defining `override val source` and related
  * implementation of `SourceSettings` Use `derives ReadWriter` on the concrete implementation to enable automatic
  * context deserialization.
  */
abstract class DefaultPluginStreamContext(
    final override val observability: DefaultObservabilitySettings,
    final override val staging: DefaultStagingSettings,
    final override val streamMode: DefaultStreamModeSettings,
    final override val sink: DefaultSinkSettings,
    final override val throughput: DefaultThroughputSettings
) extends PluginStreamContext
