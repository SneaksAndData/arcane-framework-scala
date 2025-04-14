package com.sneaksanddata.arcane.framework
package services.base

import services.metrics.ArcaneDimensionsProvider

import zio.{ZIO, ZLayer}
import zio.metrics.Metric.Counter
import zio.metrics.{Metric, MetricLabel}

/**
 * A object that contains the declared metrics names.
 */
class DeclaredMetrics(dimensionsProvider: DimensionsProvider):

  /**
   * The namespace for the metrics. Prefixes the metric name with the namespace. For now, it is not configurable.
   */
  private val metricsNamespace = "arcane.stream"

  /**
   * Number of rows received from the source
   */
  val rowsIncoming: Counter[Long] = Metric.counter(s"$metricsNamespace.rows.incoming")
    .tagged(dimensionsProvider.getDimensions.toMetricsLabelSet)


  extension (labels: Map[String, String]) private def toMetricsLabelSet: Set[MetricLabel] =
    labels.map{ case (key, value) => MetricLabel(key, value) }.toSet

object DeclaredMetrics:

  /**
   * The environment type for the DeclaredMetrics.
   */
  type Environment = ArcaneDimensionsProvider

  /**
   * Creates a new instance of the DeclaredMetrics.
   *
   * @param dimensionsProvider The stream context.
   * @return The ArcaneDimensionsProvider instance.
   */
  def apply(dimensionsProvider: DimensionsProvider): DeclaredMetrics = new DeclaredMetrics(dimensionsProvider)
  
  /**
   * The ZLayer that creates the DeclaredMetrics.
   */
  val layer: ZLayer[Environment, Nothing, DeclaredMetrics] =
    ZLayer {
      for
        dimensionsProvider <- ZIO.service[DimensionsProvider]
      yield DeclaredMetrics(dimensionsProvider)
    }