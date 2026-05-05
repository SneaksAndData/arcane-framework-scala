package com.sneaksanddata.arcane.framework
package services.metrics.base

import scala.collection.immutable.SortedMap

/** Provides the metrics tags.
  */
trait MetricTagProvider:
  /** Retrieves metric tags associated with this provider
    */
  def getTags: SortedMap[String, String]
