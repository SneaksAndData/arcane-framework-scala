package com.sneaksanddata.arcane.framework
package models.settings

/**
 * Provides settings for a stream sink.
 */
trait SinkSettings:
  
  /**
   * The target table to write the data.
   */
  val sinkLocation: String
