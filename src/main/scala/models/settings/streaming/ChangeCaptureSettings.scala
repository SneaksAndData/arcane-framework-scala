package com.sneaksanddata.arcane.framework
package models.settings.streaming

import java.time.Duration

/** Provides settings for a stream source.
  */
trait ChangeCaptureSettings:

  /** The interval for periodic change capture operation.
    */
  val changeCaptureInterval: Duration

  /** Variance to apply to the `changeCaptureInterval`
    */
  val changeCaptureJitterVariance: Double

  /** Seed for `changeCaptureJitterVariance`
    */
  val changeCaptureJitterSeed: Long
