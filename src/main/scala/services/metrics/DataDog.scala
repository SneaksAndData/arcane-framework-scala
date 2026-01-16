package com.sneaksanddata.arcane.framework
package services.metrics

import zio.ZLayer
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.{DatagramSocketConfig, StatsdClient}
import zio.metrics.connectors.{MetricsConfig, datadog, statsd}

/** DataDog metrics configuration and layer setup. This module provides the necessary configurations and layers to
  * integrate DataDog metrics into the application using ZIO.
  */
object DataDog {

  /** Environment required to run the DataDog metrics layer.
    */
  type Environment = DatagramSocketConfig & MetricsConfig & DatadogPublisherConfig

  object UdsPublisher {

    /** Layer that provides the DataDog metrics configuration.
      */
    val layer: ZLayer[Environment, Nothing, Unit] = statsd.statsdUDS >>> datadog.live

  }
}
