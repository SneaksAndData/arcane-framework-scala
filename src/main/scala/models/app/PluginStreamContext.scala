package com.sneaksanddata.arcane.framework
package models.app

import models.settings.observability.ObservabilitySettings
import models.settings.sink.SinkSettings
import models.settings.sources.StreamSourceSettings
import models.settings.staging.StagingSettings
import models.settings.streaming.{StreamModeSettings, ThroughputSettings}

import com.sneaksanddata.arcane.framework.models.app.PluginStreamContext.PluginConfiguration
import upickle.ReadWriter
import zio.{IO, ZIO, ZLayer}
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

  def merge[OtherImpl <: OverrideStreamContext](other: Option[OtherImpl]): this.type

object PluginStreamContext:
  def apply[Spec <: PluginStreamContext](value: String)(implicit rw: ReadWriter[Spec]): Spec = upickle.read(value)

  private def fromEnvironment[Spec <: PluginStreamContext](envVarName: String)(implicit rw: ReadWriter[Spec] ): IO[SecurityException, Option[Spec]] =
    zio.System.env(envVarName).flatMap {
      case Some(value) => ZIO.succeed(Some(apply(value)))
      case None => ZIO.succeed(None)
    }


  given Conversion[PluginStreamContext, DatagramSocketConfig] with
    def apply(spec: PluginStreamContext): DatagramSocketConfig =
      DatagramSocketConfig(spec.datadogSocketPath)

  given Conversion[PluginStreamContext, MetricsConfig] with
    def apply(spec: PluginStreamContext): MetricsConfig =
      MetricsConfig(spec.metricsPublisherInterval)

  type PluginConfiguration = PluginStreamContext & DatagramSocketConfig & MetricsConfig & DatadogPublisherConfig

  /** Parses and initializes StreamContext for the plugin. This should be used when defining `layer` for plugin context
    * injection. You can also specify additional services or options to be added: object MyContext: val layer =
    * spec.loadContext() ++ ZLayer.succeed(MySourceConnectionOptions)
    */
  def getLayer[ContextImpl <: PluginConfiguration, OverridesImpl <: OverrideStreamContext](implicit
      rwc: ReadWriter[ContextImpl],
      rwo: ReadWriter[OverridesImpl]
  ): ZLayer[Any, Throwable, PluginConfiguration] =

    val effect = for
        context <- PluginStreamContext.fromEnvironment[ContextImpl]("STREAMCONTEXT__SPEC")
        contextOverrides <- OverrideStreamContext.fromEnvironmentOverrides[OverridesImpl]("STREAMCONTEXT_SPEC_OVERRIDE")
    yield context match
      case Some(parsed) => parsed.merge[OverridesImpl](contextOverrides)
      case None => throw new Throwable( s"Unable to resolve stream context. Please verify that STREAMCONTEXT__SPEC is defined as a valid JSON string." )

    ZLayer.fromZIO[Any, Throwable, PluginStreamContext](effect)
      ++ ZLayer.fromZIO[Any, Throwable, DatagramSocketConfig](effect)
      ++ ZLayer.fromZIO[Any, Throwable, MetricsConfig](effect)
      ++ ZLayer.succeed(DatadogPublisherConfig())

