package com.sneaksanddata.arcane.framework
package models.settings.streaming

import models.serialization.JavaDurationRW.*

import upickle.ReadWriter
import upickle.default.*

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

case class DefaultChangeCaptureSettings(
                                         override val changeCaptureJitterSeed: Long,
                                         override val changeCaptureJitterVariance: Double,
                                         override val changeCaptureInterval: Duration
                                       ) extends ChangeCaptureSettings derives ReadWriter