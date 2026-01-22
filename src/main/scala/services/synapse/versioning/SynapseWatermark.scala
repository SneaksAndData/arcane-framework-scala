package com.sneaksanddata.arcane.framework
package services.synapse.versioning

import services.streaming.base.OffsetDateTimeRW.*
import services.streaming.base.{JsonWatermark, SourceWatermark}
import services.synapse.versioning.SynapseVersionExtensions.*

import upickle.core.Visitor
import upickle.default.*

import java.time.{Instant, OffsetDateTime, ZoneOffset}

type SynapseVersionType = String

case class SynapseWatermark(version: SynapseVersionType, timestamp: OffsetDateTime, prefix: String)
    extends SourceWatermark[SynapseVersionType]
    with JsonWatermark:
  override def compare(that: SourceWatermark[SynapseVersionType]): Int =
    (version.asDate.toEpochSecond, that.version.asDate.toEpochSecond) match
      case (x, y) if x < y  => -1
      case (x, y) if x == y => 0
      case (x, y) if x > y  => 1

  override def toJson: String = upickle.write(this)

object SynapseWatermark:
  implicit val rw: ReadWriter[SynapseWatermark] = macroRW

  def fromJson(value: String): SynapseWatermark = upickle.read(value)

  /** EPOCH watermark is used as a "null" watermark
    */
  val epoch: SynapseWatermark =
    val start = OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)
    SynapseWatermark(version = "", timestamp = start, prefix = "")
