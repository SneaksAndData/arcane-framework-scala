package com.sneaksanddata.arcane.framework
package models.app

import models.settings.observability.ObservabilitySettings
import models.settings.sink.SinkSettings
import models.settings.sources.StreamSourceSettings
import models.settings.staging.StagingSettings
import models.settings.streaming.{StreamModeSettings, ThroughputSettings}

import upickle.ReadWriter
import zio.ZLayer
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
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

  private def fromEnvironment[Spec <: PluginStreamContext](envVarName: String)(implicit
      rw: ReadWriter[Spec]
  ): Option[Spec] =
    sys.env.get(envVarName).map(env => apply(env))

  given Conversion[PluginStreamContext, DatagramSocketConfig] with
    def apply(spec: PluginStreamContext): DatagramSocketConfig =
      DatagramSocketConfig(spec.datadogSocketPath)

  given Conversion[PluginStreamContext, MetricsConfig] with
    def apply(spec: PluginStreamContext): MetricsConfig =
      MetricsConfig(spec.metricsPublisherInterval)

  private type Services = PluginStreamContext & DatagramSocketConfig & MetricsConfig & DatadogPublisherConfig

  /** Parses and initializes StreamContext for the plugin. This should be used when defining `layer` for plugin context
    * injection. You can also specify additional services or options to be added: object MyContext: val layer =
    * spec.loadContext() ++ ZLayer.succeed(MySourceConnectionOptions)
    */
  def getLayer[ContextImpl <: PluginStreamContext](implicit
      rw: ReadWriter[ContextImpl]
  ): ZLayer[Any, Throwable, Services] =
    val context = PluginStreamContext
      .fromEnvironment[ContextImpl]("STREAMCONTEXT__SPEC")

    val contextOverrides = PluginStreamContext
      .fromEnvironment[ContextImpl]("STREAMCONTEXT_SPEC_OVERRIDE")

    context
      .map(parsed =>
        val merged = parsed.merge(contextOverrides)
        ZLayer.succeed(merged) ++ ZLayer.succeed[DatagramSocketConfig](merged) ++ ZLayer
          .succeed[MetricsConfig](merged) ++ ZLayer.succeed(DatadogPublisherConfig())
      )
      .getOrElse(
        ZLayer.fail(
          new Throwable(
            "Unable to resolve stream context. Please verify that STREAMCONTEXT__SPEC is defined as a valid JSON string."
          )
        )
      )
