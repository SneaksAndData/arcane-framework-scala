package com.sneaksanddata.arcane.framework
package services.base

import zio.metrics.Metric
import zio.metrics.Metric.Counter

/**
 * A object that contains the declared metrics names.
 */
object DeclaredMetrics:
  /**
   * Number of rows received from the source
   */
  val rowsIncoming: Counter[Long] = Metric.counter("rows_incoming")