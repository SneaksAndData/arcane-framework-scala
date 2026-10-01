package com.sneaksanddata.arcane.framework
package models.settings.streaming

import models.serialization.JavaDurationRW.*

import upickle.ReadWriter

import java.time.Duration

/** A partial override of `ChangeCaptureSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideChangeCaptureSettings:

  /** Optional override for the change capture interval.
    */
  val changeCaptureInterval: Option[Duration]

  /** Optional override for the jitter variance applied to the change capture interval.
    */
  val changeCaptureJitterVariance: Option[Double]

  /** Optional override for the jitter seed used to generate variance.
    */
  val changeCaptureJitterSeed: Option[Long]

  /** Optional override for the maximum range limit included in a single changeset.
    */
  val changeCaptureRangeLimit: Option[Int]

/** Default implementation for `OverrideChangeCaptureSettings` using optional values.
  */
case class DefaultOverrideChangeCaptureSettings(
    override val changeCaptureInterval: Option[Duration] = None,
    override val changeCaptureJitterVariance: Option[Double] = None,
    override val changeCaptureJitterSeed: Option[Long] = None,
    override val changeCaptureRangeLimit: Option[Int] = None
) extends OverrideChangeCaptureSettings derives ReadWriter
