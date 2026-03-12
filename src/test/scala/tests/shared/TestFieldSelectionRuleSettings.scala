package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.{AllFields, AllFieldsImpl, FieldSelectionRule, FieldSelectionRuleSettings}

object TestFieldSelectionRuleSettings extends FieldSelectionRuleSettings:
  override val rule: FieldSelectionRule     = AllFieldsImpl(AllFields())
  override val essentialFields: Set[String] = Set()
  override val isServerSide: Boolean        = false
