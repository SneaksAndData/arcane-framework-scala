package com.sneaksanddata.arcane.framework
package models.settings

import java.time.Duration

/** Provides settings for a stream source.
  */
trait VersionedDataGraphBuilderSettings:

  /** The interval for periodic change capture operation.
    */
  val changeCaptureInterval: Duration
