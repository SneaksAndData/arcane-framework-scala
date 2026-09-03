package com.sneaksanddata.arcane.framework
package models.settings.sources

import models.settings.{FieldSelectionRuleSettings, Mergeable}

import upickle.ReadWriter
import upickle.default.*

trait StreamSourceSettings:
  type SourceSettingsType <: SourceSettings

  val configuration: SourceSettingsType

  val buffering: SourceBufferingSettings

  val fieldSelectionRule: FieldSelectionRuleSettings

case class DefaultStreamSourceSettings(
    override val configuration: SourceSettings,
    override val buffering: SourceBufferingSettings,
    override val fieldSelectionRule: FieldSelectionRuleSettings
) extends StreamSourceSettings,
      Mergeable derives ReadWriter:
  override type SourceSettingsType = SourceSettings
  override type MergeableFrom = OverrideStreamSourceSettings
  override type MergeResult   = DefaultStreamSourceSettings

  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    DefaultStreamSourceSettings(
      configuration = overrides.flatMap(_.configuration).getOrElse(this.configuration),
      buffering = overrides.flatMap(_.buffering).getOrElse(this.buffering),
      fieldSelectionRule = overrides.flatMap(_.fieldSelectionRule).getOrElse(this.fieldSelectionRule)
    )
