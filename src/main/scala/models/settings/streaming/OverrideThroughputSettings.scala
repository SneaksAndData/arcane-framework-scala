package com.sneaksanddata.arcane.framework
package models.settings.streaming

import models.settings.FlowRate

import upickle.ReadWriter

/** A partial override of `ThroughputSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideThroughputSettings:
  /** Optional override for the shaper implementation used to manage throughput.
    */
  val shaperImpl: Option[ThroughputShaperImpl]

  /** Optional override for the recommended chunk size.
    */
  val advisedChunkSize: Option[Int]

  /** Optional override for the recommended flow rate.
    */
  val advisedRate: Option[FlowRate]

  /** Optional override for the recommended burst size.
    */
  val advisedBurst: Option[Int]

/** Default implementation for `OverrideThroughputSettings` using optional values.
  */
case class DefaultOverrideThroughputSettings(
    override val shaperImpl: Option[ThroughputShaperImpl] = None,
    override val advisedChunkSize: Option[Int] = None,
    override val advisedRate: Option[FlowRate] = None,
    override val advisedBurst: Option[Int] = None
) extends OverrideThroughputSettings derives ReadWriter
