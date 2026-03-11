package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.{AllFields, FieldSelectionRule, FieldSelectionRuleSettings}

object TestFieldSelectionRuleSettings extends FieldSelectionRuleSettings:
  override val rule: FieldSelectionRule     = AllFields()
  override val essentialFields: Set[String] = Set()
  override val isServerSide: Boolean        = false
