package com.sneaksanddata.arcane.framework
package services.metrics.base

import scala.collection.immutable.SortedMap

/**
 * A trait that represents a metrics service.
 */
trait MetricsService:

  /**
   * Increments the counter.
   *
   * @param name The name of the counter.
   * @param value The value to increment the counter by.
   */
  def incrementCounter(name: String, dimensions: SortedMap[String, String], value: Long): Unit