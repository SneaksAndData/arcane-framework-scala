package com.sneaksanddata.arcane.framework
package models.settings.observability

import models.settings.Mergeable

import upickle.{ReadWriter, macroRW}

trait ObservabilitySettings:
  /** Custom metric tags
    */
  val metricTags: Map[String, String] = Map.empty

case class DefaultObservabilitySettings(
    override val metricTags: Map[String, String]
) extends ObservabilitySettings,
      Mergeable[DefaultObservabilitySettings] derives ReadWriter:
  def merge(base: DefaultObservabilitySettings, overrides: DefaultObservabilitySettings): DefaultObservabilitySettings =
    DefaultObservabilitySettings(
      metricTags = overrides.metricTags
    )
