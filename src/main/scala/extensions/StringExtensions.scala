package com.sneaksanddata.arcane.framework
package extensions

/** String extension methods for various utility functions.
  */
object StringExtensions:

  /** Converts a string from camelCase to snake_case.
    *
    * @param str
    *   The input string in camelCase format.
    * @return
    *   The converted string in snake_case format.
    */
  extension (str: String)
    def camelCaseToSnakeCase: String = str
      .replaceAll("([a-z])([A-Z])", "$1_$2")
      .toLowerCase()
