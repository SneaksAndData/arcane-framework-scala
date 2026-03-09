package com.sneaksanddata.arcane.framework
package models.settings.observability

trait ObservabilitySettings:
  /** Custom metric tags
    */
  val metricTags: Map[String, String] = Map.empty
