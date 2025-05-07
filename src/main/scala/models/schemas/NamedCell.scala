package com.sneaksanddata.arcane.framework
package models.schemas

/**
 * Represents a row of data or schema with name assigned to it.
 */
trait NamedCell[A]:
  /**
   * Gets the name of the value.
   *
   * @param a The value to convert.
   * @return The SQL expression.
   */
  extension (a: A) def name: String
  

