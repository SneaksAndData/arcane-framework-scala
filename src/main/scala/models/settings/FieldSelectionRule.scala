package com.sneaksanddata.arcane.framework
package models.settings


/**
 * Marker trait for a field selection rule classes
 */
trait FieldSelectionRule

/**
 * Include all fields in the selection
 */
trait AllFields extends FieldSelectionRule

/**
 * Include selected fields in the selection
 */
trait IncludeFields extends FieldSelectionRule:
  val fields: Set[String]

/**
 * Exclude selected fields from the selection
 */
trait ExcludeFields extends FieldSelectionRule:
  val fields: Set[String]
