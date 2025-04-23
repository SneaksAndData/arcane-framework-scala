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
  /**
   * The field selection rule to use.
   */
  val rule: FieldSelectionRule

  /**
   * The set of essential fields that must ALWAYS be included in the field selection rule.
   * Fields from this list are used in SQL queries and ALWAYS must be present in the result set.
   * This list is provided by the Arcane streaming plugin and should not be configurable.
   */
  val essentialFields: Set[String]

