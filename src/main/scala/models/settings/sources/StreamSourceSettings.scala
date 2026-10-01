package com.sneaksanddata.arcane.framework
package models.settings.sources

import models.settings.sources.modification.DataRowModificationSettings
import models.settings.{FieldSelectionRuleSettings, Mergeable}

trait StreamSourceSettings extends Mergeable:
  type SourceSettingsType <: SourceSettings

  val configuration: SourceSettingsType

  val buffering: SourceBufferingSettings

  val fieldSelectionRule: FieldSelectionRuleSettings

  val modifications: DataRowModificationSettings

  override type MergeableFrom = OverrideStreamSourceSettings
