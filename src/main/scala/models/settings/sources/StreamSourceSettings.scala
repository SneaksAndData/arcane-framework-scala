package com.sneaksanddata.arcane.framework
package models.settings.sources

import models.settings.{DefaultFieldSelectionRuleSettings, FieldSelectionRule, FieldSelectionRuleSettings}

import upickle.ReadWriter
import upickle.default.*

trait StreamSourceSettings:
  type SourceSettingsType <: SourceSettings

  val source: SourceSettingsType

  val buffering: SourceBufferingSettings

  val fieldSelectionRule: FieldSelectionRuleSettings
