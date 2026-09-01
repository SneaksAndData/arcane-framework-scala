package com.sneaksanddata.arcane.framework
package models.app

import models.settings.observability.ObservabilitySettings
import models.settings.sink.SinkSettings
import models.settings.sources.StreamSourceSettings
import models.settings.staging.StagingSettings
import models.settings.streaming.{OverrideStreamModeSettings, ThroughputSettings}

import upickle.ReadWriter
import zio.ZLayer
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig

/** The stream mode that can be overridden by the stream override provided by the arcane operator with the environment
  * variable.
  */
trait OverrideStreamContext:
  val streamMode: Option[OverrideStreamModeSettings]
