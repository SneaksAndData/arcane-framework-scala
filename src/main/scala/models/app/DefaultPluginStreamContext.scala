package com.sneaksanddata.arcane.framework
package models.app

import models.settings.backfill.{BackfillBehavior, DefaultBackfillSettings}
import models.settings.iceberg.DefaultIcebergStagingSettings
import models.settings.observability.DefaultObservabilitySettings
import models.settings.sink.{DefaultSinkSettings, IcebergSinkSettings, TableMaintenanceSettings}
import models.settings.sources.{BufferingStrategy, StreamSourceSettings}
import models.settings.staging.{DefaultStagingSettings, JdbcQueryRetryMode}
import models.settings.streaming.{DefaultStreamModeSettings, DefaultThroughputSettings, ThroughputSettings, ThroughputShaperImpl}
import models.settings.{FieldSelectionRule, TableFormat}

import upickle.{ReadWriter, macroRW}

import java.time.{Duration, OffsetDateTime}

case class DefaultPluginStreamContext(
    override val observability: DefaultObservabilitySettings,
    override val source: StreamSourceSettings,
    override val staging: DefaultStagingSettings,
    override val streamMode: DefaultStreamModeSettings,         
    override val sink: DefaultSinkSettings,
    override val throughput: DefaultThroughputSettings
) extends PluginStreamContext:

  // observability
  override def customTags: Map[String, String] = observability.metricTags

  override def merge(other: Option[PluginStreamContext]): PluginStreamContext = ???
