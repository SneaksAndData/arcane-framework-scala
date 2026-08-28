package com.sneaksanddata.arcane.framework
package models.settings.streaming

import models.serialization.JavaDurationRW.*

import com.sneaksanddata.arcane.framework.models.settings.Mergeable
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

  /** Maximum time difference allowed between latest and watermarked version to be included in a single changeset.
    */
  val changeCaptureRangeLimit: Int

case class DefaultChangeCaptureSettings(
    override val changeCaptureJitterSeed: Long,
    override val changeCaptureJitterVariance: Double,
    override val changeCaptureInterval: Duration,
    override val changeCaptureRangeLimit: Int
) extends ChangeCaptureSettings,
      Mergeable[DefaultChangeCaptureSettings] derives ReadWriter:
  override def merge(
      base: DefaultChangeCaptureSettings,
      overrides: DefaultChangeCaptureSettings
  ): DefaultChangeCaptureSettings =
    DefaultChangeCaptureSettings(
      changeCaptureJitterSeed = overrides.changeCaptureJitterSeed,
      changeCaptureJitterVariance = overrides.changeCaptureJitterVariance,
      changeCaptureInterval = overrides.changeCaptureInterval,
      changeCaptureRangeLimit = overrides.changeCaptureRangeLimit
    )
