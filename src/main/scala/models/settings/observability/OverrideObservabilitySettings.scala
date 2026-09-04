package com.sneaksanddata.arcane.framework
package models.settings.observability

import upickle.ReadWriter

/** A partial override of `ObservabilitySettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideObservabilitySettings:
  /** Optional override for custom metric tags.
    */
  val metricTags: Option[Map[String, String]]

/** Default implementation for `OverrideObservabilitySettings` using optional values.
  */
case class DefaultOverrideObservabilitySettings(
    override val metricTags: Option[Map[String, String]] = None
) extends OverrideObservabilitySettings derives ReadWriter
