package com.sneaksanddata.arcane.framework
package models.settings

import java.time.Duration

/**
 * Provides grouping settings for the stream
 */
trait GroupingSettings {

  /**
   * The interval to group the data.
   */
  val groupingInterval: Duration

  /**
   * The number of rows per group.
   */
  val rowsPerGroup: Int
}
