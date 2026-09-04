package com.sneaksanddata.arcane.framework
package models.settings.streaming

import models.settings.FlowRate
import models.serialization.FlowRateRW.*

import upickle.default.*
import upickle.ReadWriter
import upickle.implicits.key

/** A partial override of `ThroughputSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideThroughputSettings:
  /** Optional override for the shaper implementation.
    */
  val shaperImplSetting: Option[ThroughputShaperImplSettings]

  /** Optional override for the advised chunk size.
    */
  val advisedChunkSize: Option[Int]

  /** Optional override for the advised flow rate.
    */
  val advisedRate: Option[FlowRate]

  /** Optional override for the advised burst size.
    */
  val advisedBurst: Option[Int]

/** Default implementation for `OverrideThroughputSettings` using optional values.
  */
case class DefaultOverrideThroughputSettings(
    @key("shaperImpl") override val shaperImplSetting: Option[ThroughputShaperImplSettings] = None,
    override val advisedChunkSize: Option[Int] = None,
    override val advisedRate: Option[FlowRate] = None,
    override val advisedBurst: Option[Int] = None
) extends OverrideThroughputSettings derives ReadWriter
