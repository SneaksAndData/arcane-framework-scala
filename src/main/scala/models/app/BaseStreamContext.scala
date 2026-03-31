package com.sneaksanddata.arcane.framework
package models.app

import upickle.ReadWriter
import zio.{IO, ZIO, ZLayer}
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig

import java.time.Duration

/** Provides the context for the stream.
  */
trait BaseStreamContext:

  /** The id of the stream.
    */
  def streamId: IO[SecurityException, String] = zio.System.env("STREAMCONTEXT__STREAM_ID").map {
    case Some(value) => value
    case None =>
      throw new RuntimeException(
        "Unable to bootstrap the stream, missing required STREAMCONTEXT__STREAM_ID environment variable"
      )
  }

  /** True if the stream is running in backfill mode.
    */
  def isBackfilling: ZIO[Any, SecurityException, Boolean] =
    zio.System.envOrElse("STREAMCONTEXT__BACKFILL", "false").map(_.toLowerCase() == "true")

  /** Kind of the stream
    */
  def streamKind: IO[SecurityException, String] = zio.System.env("STREAMCONTEXT__STREAM_KIND").map {
    case Some(value) => value
    case None =>
      throw new RuntimeException(
        "Unable to bootstrap the stream, missing required STREAMCONTEXT__STREAM_KIND environment variable"
      )
  }

  val datadogSocketPath: String =
    sys.env.getOrElse("ARCANE_FRAMEWORK__DATADOG_SOCKET_PATH", "/var/run/datadog/dsd.socket")

  val metricsPublisherInterval: Duration = Duration.ofMillis(
    sys.env.getOrElse("ARCANE_FRAMEWORK__METRICS_PUBLISHER_INTERVAL_MILLIS", "100").toInt
  )
