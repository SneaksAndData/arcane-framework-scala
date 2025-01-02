package com.sneaksanddata.arcane.framework
package services.streaming.base

/**
 * Type class that represents the ability to get the latest version of a data.
 * @tparam Data The type of the data.
 */
trait HasVersion[Data] {

  type VersionType

  /**
   * Gets the latest version of the data.
   *
   * @return The latest version of the data.
   */
  extension (a: Data) def getLatestVersion: VersionType
}
