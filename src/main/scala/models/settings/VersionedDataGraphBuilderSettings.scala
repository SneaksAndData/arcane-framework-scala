package com.sneaksanddata.arcane.framework
package models.settings

import java.time.Duration

/**
 * Provides settings for a stream source.
 */
trait VersionedDataGraphBuilderSettings:

  /**
   * The interval to look back for changes on the startup of the stream.
   */
  val lookBackInterval: Duration

  /**
   * The interval for periodic change capture operation.
   */
  val changeCaptureInterval: Duration
