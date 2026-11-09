package com.sneaksanddata.arcane.framework
package models.settings.sources

import models.settings.sources.modification.{
  DefaultDataRowModificationSettings,
  DefaultOverrideDataRowModificationSettings
}

trait OverrideStreamSourceSettings:
  type SourceSettingsOverrideType <: SourceSettings

  val configuration: Option[SourceSettingsOverrideType]

  val buffering: Option[DefaultOverrideSourceBufferingSettings]

  val modifications: Option[DefaultOverrideDataRowModificationSettings]
