package com.sneaksanddata.arcane.framework
package models.settings.sources

import models.settings.{DefaultFieldSelectionRuleSettings, FieldSelectionRule, FieldSelectionRuleSettings}

import upickle.ReadWriter

trait StreamSourceSettings extends SourceSettings with SourceBufferingSettings with FieldSelectionRuleSettings

case class DefaultStreamSourceSettings(
    buffering: DefaultSourceBufferingSettings,
    fieldSelectionRuleSettings: DefaultFieldSelectionRuleSettings,
    changeCaptureInterval: Int
) extends StreamSourceSettings derives ReadWriter {

  override val rule: FieldSelectionRule     = fieldSelectionRuleSettings.rule
  override val essentialFields: Set[String] = fieldSelectionRuleSettings.essentialFields
  override val isServerSide: Boolean        = fieldSelectionRuleSettings.isServerSide

  override val bufferingStrategy: BufferingStrategy = buffering.bufferingStrategy
  override val bufferingEnabled: Boolean            = buffering.bufferingEnabled

  override val changeCaptureIntervalSeconds: Int = changeCaptureInterval
}
