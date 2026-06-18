package com.sneaksanddata.arcane.framework
package services.metrics

import services.metrics.base.MetricTagProvider

import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.{DatagramSocketConfig, StatsdClient, statsdUDS}
import zio.metrics.connectors.{MetricsConfig, datadog}
import zio.metrics.jvm.DefaultJvmMetrics
import zio.{URLayer, ZIO, ZIOAspect, ZLayer}

import scala.collection.SortedMap

/** DataDog metrics configuration and layer setup. This module provides the necessary configurations and layers to
  * integrate DataDog metrics into the application using ZIO.
  */
object DataDog {

  /** Environment required to run the DataDog metrics layer.
    */
  type Environment = DatagramSocketConfig & MetricsConfig & DatadogPublisherConfig

  object UdsPublisher {

    private val udsLayer: URLayer[DatagramSocketConfig & MetricsConfig, StatsdClient] = statsdUDS

    /** Layer that provides the DataDog metrics configuration.
      */
    val layer: ZLayer[Environment, Nothing, Unit] = udsLayer >>> datadog.live

    val jvmLayer = ZLayer {
      for tagProvider <- ZIO.service[MetricTagProvider]
      yield (DefaultJvmMetrics.liveV2 >>> statsdUDS >>> datadog.live).build @@ ZIOAspect.tagged(
        Option(tagProvider.getTags).getOrElse(SortedMap.empty[String, String]).toList*
      )
    }

  }
}
