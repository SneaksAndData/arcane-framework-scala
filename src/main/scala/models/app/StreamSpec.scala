package com.sneaksanddata.arcane.framework
package models.app

import models.settings.backfill.BackfillSettings
import models.settings.iceberg.IcebergStagingSettings
import models.settings.sink.SinkSettings
import models.settings.sources.StreamSourceSettings
import models.settings.staging.StagingDataSettings
import models.settings.{
  FieldSelectionRuleSettings,
  JdbcMergeServiceClientSettings,
  TablePropertiesSettings,
  VersionedDataGraphBuilderSettings
}

import upickle.ReadWriter
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig

/** Stream specification must be implemented by the plugin. It is not used by framework directly.
  */
trait StreamSpec
    extends StreamContext
    with BackfillSettings
    with SinkSettings
    with StreamSourceSettings
    with FieldSelectionRuleSettings
    with StagingDataSettings
    with VersionedDataGraphBuilderSettings
    with TablePropertiesSettings
    with IcebergStagingSettings
    with JdbcMergeServiceClientSettings

object StreamSpec:
  def apply(value: String)(implicit rw: ReadWriter[StreamSpec]): StreamSpec = upickle.read(value)

  def fromEnvironment(envVarName: String)(implicit rw: ReadWriter[StreamSpec]): Option[StreamSpec] =
    sys.env.get(envVarName).map(env => apply(env))

  given Conversion[StreamSpec, DatagramSocketConfig] with
    def apply(spec: StreamSpec): DatagramSocketConfig =
      DatagramSocketConfig(spec.datadogSocketPath)

  given Conversion[StreamSpec, MetricsConfig] with
    def apply(spec: StreamSpec): MetricsConfig =
      MetricsConfig(spec.metricsPublisherInterval)
