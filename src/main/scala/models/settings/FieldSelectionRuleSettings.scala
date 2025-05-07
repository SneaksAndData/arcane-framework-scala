package com.sneaksanddata.arcane.framework
package models.settings

/** Represents a field selection rule for a streaming batch. The field selection rule is used to determine which fields
  * should be included in the result set of a query.
  */
enum FieldSelectionRule:
  /** All fields should be included in the result set.
    */
  case AllFields

  /** Only the specified fields should be excluded from the result set.
    */
  case IncludeFields(fields: Set[String])

  /** All fields except the specified fields should be included in the result set.
    */
  case ExcludeFields(fields: Set[String])

/** Marker trait for a field selection rule classes
  */
trait FieldSelectionRuleSettings:
  /** The field selection rule to use.
    */
  val rule: FieldSelectionRule

  /** The set of essential fields that must ALWAYS be included in the field selection rule. Fields from this list are
    * used in SQL queries and ALWAYS must be present in the result set. This list is provided by the Arcane streaming
    * plugin and should not be configurable.
    */
  val essentialFields: Set[String]
