package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.observability.ObservabilitySettings

case object TestObservabilitySettings extends ObservabilitySettings:
  override val metricTags: Map[String, String] = Map.empty
