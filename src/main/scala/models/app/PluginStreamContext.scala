package com.sneaksanddata.arcane.framework
package models.app

import models.settings.backfill.BackfillSettings
import models.settings.iceberg.IcebergStagingSettings
import models.settings.observability.ObservabilitySettings
import models.settings.sink.SinkSettings
import models.settings.sources.{SourceSettings, StreamSourceSettings}
import models.settings.staging.{JdbcMergeServiceClientSettings, StagingSettings, StagingTableSettings}
import models.settings.streaming.{ChangeCaptureSettings, StreamModeSettings, ThroughputSettings}
import models.settings.TablePropertiesSettings

import upickle.ReadWriter
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig

/** Stream specification must be implemented by the plugin. It is not used by framework directly.
  */
trait PluginStreamContext extends BaseStreamContext:
  val streamMode: StreamModeSettings

  val sink: SinkSettings

  val source: StreamSourceSettings

  val staging: StagingSettings

  val observability: ObservabilitySettings

  val throughput: ThroughputSettings

  def merge(other: Option[PluginStreamContext]): PluginStreamContext

object PluginStreamContext:
  def apply[Spec <: PluginStreamContext](value: String)(implicit rw: ReadWriter[Spec]): Spec = upickle.read(value)

  def fromEnvironment[Spec <: PluginStreamContext](envVarName: String)(implicit rw: ReadWriter[Spec]): Option[Spec] =
    sys.env.get(envVarName).map(env => apply(env))

  given Conversion[PluginStreamContext, DatagramSocketConfig] with
    def apply(spec: PluginStreamContext): DatagramSocketConfig =
      DatagramSocketConfig(spec.datadogSocketPath)

  given Conversion[PluginStreamContext, MetricsConfig] with
    def apply(spec: PluginStreamContext): MetricsConfig =
      MetricsConfig(spec.metricsPublisherInterval)
