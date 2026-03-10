package com.sneaksanddata.arcane.framework
package models.settings.observability

import upickle.{ReadWriter, macroRW}

trait ObservabilitySettings:
  /** Custom metric tags
    */
  val metricTags: Map[String, String] = Map.empty

case class DefaultObservabilitySettings(
    override val metricTags: Map[String, String]
) extends ObservabilitySettings derives ReadWriter
