package com.sneaksanddata.arcane.framework
package utils

import models.settings.{FieldSelectionRule, FieldSelectionRuleSettings}

object TestFieldSelectionRuleSettings extends FieldSelectionRuleSettings:
  override val rule: FieldSelectionRule = FieldSelectionRule.AllFields
  override val essentialFields: Set[String] = Set()
