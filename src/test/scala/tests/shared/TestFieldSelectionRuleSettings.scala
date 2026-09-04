package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.{AllFields, AllFieldsImpl, FieldSelectionRule, FieldSelectionRuleSettings}

object TestFieldSelectionRuleSettings extends FieldSelectionRuleSettings:
  override val rule: FieldSelectionRule     = AllFieldsImpl(AllFields())
  override val essentialFields: Set[String] = Set()
  @deprecated("This setting is not used from 2.3.2 release. It will be removed in 2.4.0.")
  override val isServerSide: Boolean = false

  override type MergeableFrom = this.type
  override type MergeResult = this.type
  override def merge(overrides: Option[MergeableFrom]): MergeResult = ???
