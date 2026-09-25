package com.sneaksanddata.arcane.framework
package models.settings

import upickle.ReadWriter
import upickle.default.*
import upickle.implicits.key

/** A partial override of `FieldSelectionRuleSettings` where every field is optional to support override/patch-style
  * JSON deserialization.
  */
trait OverrideFieldSelectionRuleSettings:
  /** Optional override for the field selection rule.
    */
  val ruleSetting: Option[FieldSelectionRuleSetting]

  /** Optional override for the essential fields set.
    */
  val essentialFields: Option[Set[String]]

  /** Optional override for whether the field selection is server-side.
    */
  val isServerSide: Option[Boolean]

/** Default implementation for `OverrideFieldSelectionRuleSettings` using optional values.
  */
case class DefaultOverrideFieldSelectionRuleSettings(
    @key("rule") override val ruleSetting: Option[FieldSelectionRuleSetting] = None,
    override val essentialFields: Option[Set[String]] = None,
    override val isServerSide: Option[Boolean] = None
) extends OverrideFieldSelectionRuleSettings derives ReadWriter
