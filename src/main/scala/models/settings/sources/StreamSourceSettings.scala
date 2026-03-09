package com.sneaksanddata.arcane.framework
package models.settings.sources

import models.settings.{DefaultFieldSelectionRuleSettings, FieldSelectionRule, FieldSelectionRuleSettings}

import upickle.ReadWriter

trait StreamSourceSettings:
  type SourceSettingsType <: SourceSettings
  
  val source: SourceSettingsType
  
  val buffering: SourceBufferingSettings
  
  val fieldSelectionRule: FieldSelectionRuleSettings

case class DefaultStreamSourceSettings(
                                                                              override val buffering: SourceBufferingSettings,
                                                                              override val fieldSelectionRule: FieldSelectionRuleSettings,
                                                                              override val source: StreamSourceSettings#SourceSettingsType
) extends StreamSourceSettings derives ReadWriter
