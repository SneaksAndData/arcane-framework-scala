package com.sneaksanddata.arcane.framework
package services.metrics.datadog

import services.metrics.base.MetricsService
import services.metrics.datadog.base.DataDogClientBuilder

import zio.{ZIO, ZLayer}

import scala.collection.immutable.SortedMap


/**
 * A metrics service that sends metrics to DataDog.
 *
 * @param dataDogClientBuilder The DataDog configuration.
 */
class DatadogMetricsService(dataDogClientBuilder: DataDogClientBuilder) extends MetricsService with AutoCloseable:

  private val dataDog = dataDogClientBuilder.build

  /**
   * Increments the counter.
   *
   * @param name The name of the counter.
   * @param value The value to increment the counter by.
   */
  def incrementCounter(name: String, dimensions: SortedMap[String, String], value: Long): Unit =
    dataDog.increment(name, value, dimensions.toTags*)


  /**
   * Closes the metrics service.
   */
  override def close(): Unit = dataDog.close()

  extension (a: SortedMap[String, String]) private def toTags: List[String] = a.map { case (k, v) => s"$k:$v" }.toList

/**
 * The companion object for the DatadogMetricsService class.
 */
object DatadogMetricsService:

  /**
   * The ZLayer that creates the LazyOutputDataProcessor.
   */
  val layer: ZLayer[DataDogClientBuilder, Nothing, MetricsService] =
    ZLayer {
      for
        configuration <- ZIO.service[DataDogClientBuilder]
      yield DatadogMetricsService(configuration)
    }

  /**
   * Creates a new instance of the DatadogMetricsService.
   *
   * @param datadogClientBuilder The DataDog client.
   * @return The DatadogMetricsService instance.
   */
  def apply(datadogClientBuilder: DataDogClientBuilder): DatadogMetricsService =
    new DatadogMetricsService(datadogClientBuilder)
