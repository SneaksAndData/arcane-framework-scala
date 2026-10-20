package com.sneaksanddata.arcane.framework
package models.settings.sources

import models.settings.sources.modification.DataRowModificationSettings
import models.settings.Mergeable

trait StreamSourceSettings extends Mergeable:
  type SourceSettingsType <: SourceSettings

  val configuration: SourceSettingsType

  val buffering: SourceBufferingSettings

  val modifications: DataRowModificationSettings

  override type MergeableFrom = OverrideStreamSourceSettings
