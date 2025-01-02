package com.sneaksanddata.arcane.framework
package models.settings

import java.time.Duration

/**
 * Provides settings for a stream source.
 */
trait VersionedDataGraphBuilderSettings {

  /**
   * The interval to look back for changes if the version is empty.
   */
  val lookBackInterval: Duration

  /**
   * The interval to look back for changes if the version is empty.
   */
  val changeCaptureInterval: Duration
}
