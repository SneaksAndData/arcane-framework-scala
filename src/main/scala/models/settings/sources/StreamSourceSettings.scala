package com.sneaksanddata.arcane.framework
package models.settings.sources

import models.settings.{DefaultFieldSelectionRuleSettings, FieldSelectionRule, FieldSelectionRuleSettings}

import upickle.ReadWriter
import upickle.default.*

trait StreamSourceSettings:
  type SourceSettingsType <: SourceSettings

  val configuration: SourceSettingsType

  def recordModifications: RecordModificationSettings = DefaultRecordModificationSettings(Seq.empty)

  val buffering: SourceBufferingSettings

  val fieldSelectionRule: FieldSelectionRuleSettings
