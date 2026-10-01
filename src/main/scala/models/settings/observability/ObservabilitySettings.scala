package com.sneaksanddata.arcane.framework
package models.settings.observability

import models.settings.Mergeable

import upickle.ReadWriter

trait ObservabilitySettings extends Mergeable:
  /** Custom metric tags
    */
  val metricTags: Map[String, String] = Map.empty

case class DefaultObservabilitySettings(
    override val metricTags: Map[String, String]
) extends ObservabilitySettings,
      Mergeable derives ReadWriter:

  override type MergeableFrom = OverrideObservabilitySettings
  override type MergeResult   = DefaultObservabilitySettings

  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    DefaultObservabilitySettings(
      metricTags = overrides.flatMap(_.metricTags).getOrElse(this.metricTags)
    )
