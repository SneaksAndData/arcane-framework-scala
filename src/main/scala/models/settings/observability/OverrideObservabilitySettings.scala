package com.sneaksanddata.arcane.framework
package models.settings.observability

import upickle.ReadWriter

trait OverrideObservabilitySettings:
  /** Custom metric tags
    */
  val metricTags: Option[Map[String, String]] = None

case class DefaultOverrideObservabilitySettings(
    override val metricTags: Option[Map[String, String]] = None
) extends OverrideObservabilitySettings derives ReadWriter
