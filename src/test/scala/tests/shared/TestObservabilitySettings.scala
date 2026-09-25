package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.observability.ObservabilitySettings

case object TestObservabilitySettings extends ObservabilitySettings:
  override val metricTags: Map[String, String] = Map.empty

  override type MergeableFrom = this.type
  override type MergeResult   = this.type
  override def merge(overrides: Option[MergeableFrom]): MergeResult = ???
