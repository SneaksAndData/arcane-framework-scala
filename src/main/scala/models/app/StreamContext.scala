package com.sneaksanddata.arcane.framework
package models.app

import upickle.ReadWriter
import zio.ZLayer
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig

import java.time.Duration

/** Provides the context for the stream.
  */
trait StreamContext:

  /** The id of the stream.
    */
  def streamId: String = sys.env("STREAMCONTEXT__STREAM_ID")

  /** True if the stream is running in backfill mode.
    */
  def IsBackfilling: Boolean = sys.env.getOrElse("STREAMCONTEXT__BACKFILL", "false").toLowerCase() == "true"

  /** Kind of the stream
    */
  def streamKind: String = sys.env("STREAMCONTEXT__STREAM_KIND")

  /** User provided custom tags to be added to the metrics and logs (in future)
    */
  def customTags: Map[String, String] = Map.empty

  val datadogSocketPath: String =
    sys.env.getOrElse("ARCANE_FRAMEWORK__DATADOG_SOCKET_PATH", "/var/run/datadog/dsd.socket")

  val metricsPublisherInterval: Duration = Duration.ofMillis(
    sys.env.getOrElse("ARCANE_FRAMEWORK__METRICS_PUBLISHER_INTERVAL_MILLIS", "100").toInt
  )

object StreamContext:
  private type Environment = StreamContext & DatagramSocketConfig & MetricsConfig & DatadogPublisherConfig

  /** Parses and initializes StreamContext for the plugin. This should be used when defining `layer` for plugin context
    * injection. You can also specify additional services or options to be added: object MyContext: val layer =
    * spec.loadContext() ++ ZLayer.succeed(MySourceConnectionOptions)
    */
  extension (spec: StreamSpec)
    def loadContext(implicit rw: ReadWriter[StreamSpec]): ZLayer[Any, Throwable, Environment] =
      val spec = StreamSpec
        .fromEnvironment("STREAMCONTEXT__SPEC")

      spec
        .map(parsed =>
          ZLayer.succeed(parsed) ++ ZLayer.succeed[DatagramSocketConfig](parsed) ++ ZLayer
            .succeed[MetricsConfig](parsed) ++ ZLayer.succeed(DatadogPublisherConfig())
        )
        .getOrElse(ZLayer.fail(new Throwable("The stream context is not specified.")))
