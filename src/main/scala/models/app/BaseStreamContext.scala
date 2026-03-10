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
trait BaseStreamContext:

  /** The id of the stream.
    */
  def streamId: String = sys.env("STREAMCONTEXT__STREAM_ID")

  /** True if the stream is running in backfill mode.
    */
  def isBackfilling: Boolean = sys.env.getOrElse("STREAMCONTEXT__BACKFILL", "false").toLowerCase() == "true"

  /** Kind of the stream
    */
  def streamKind: String = sys.env("STREAMCONTEXT__STREAM_KIND")

  val datadogSocketPath: String =
    sys.env.getOrElse("ARCANE_FRAMEWORK__DATADOG_SOCKET_PATH", "/var/run/datadog/dsd.socket")

  val metricsPublisherInterval: Duration = Duration.ofMillis(
    sys.env.getOrElse("ARCANE_FRAMEWORK__METRICS_PUBLISHER_INTERVAL_MILLIS", "100").toInt
  )
