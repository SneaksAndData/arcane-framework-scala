package com.sneaksanddata.arcane.framework
package models.settings


enum FieldSelectionRule:
  case AllFields
  case IncludeFields(fields: Set[String])
  case ExcludeFields(fields: Set[String])

/**
 * Marker trait for a field selection rule classes
 */
trait FieldSelectionRuleSettings:
  val rule: FieldSelectionRule

