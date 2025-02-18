package com.sneaksanddata.arcane.framework
package services.metrics.base

import scala.collection.immutable.SortedMap

/**
 * Provides the metrics dimensions.
 */
trait DimensionsProvider:
  /**
   * Provides the dimensions.
   *
   * @return The dimensions.
   */
  def getDimensions: SortedMap[String, String]