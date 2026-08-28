package com.sneaksanddata.arcane.framework
package models.settings

/**
 * A type class that represents the ability to merge settings using the stream context overrides.
 * @tparam T The type of the settings.
 */
trait Mergeable[T]:

  /** Merges the base settings with the overrides.
   * @param base The base settings.
   * @param overrides The overrides.
   * @return The merged settings.
   */
  def merge(base: T, overrides: T): T
