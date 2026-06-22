package com.sneaksanddata.arcane.framework
package services.pushstream.versioning

import models.serialization.OffsetDateTimeRW.*
import services.streaming.base.{JsonWatermark, SourceWatermark}

import upickle.default.*

import java.time.{Instant, OffsetDateTime, ZoneOffset}

type PushStreamVersionType = String

/** PushStreamWatermark Watermark is a single timestamp.
  */
case class PushStreamWatermark(timestamp: OffsetDateTime) extends SourceWatermark[PushStreamVersionType] with JsonWatermark:
  override def compare(that: SourceWatermark[PushStreamVersionType]): Int =
    Ordering[Long].compare(version.toLong, that.version.toLong)

  override def toJson: String = upickle.write(this)

  /** Current source version associated with this watermark
   */
  override val version: PushStreamVersionType = timestamp.toEpochSecond.toString

object PushStreamWatermark:
  def fromChangeTrackingVersion(commitTime: OffsetDateTime): PushStreamWatermark = new PushStreamWatermark(
    timestamp = commitTime
  )

  implicit val rw: ReadWriter[PushStreamWatermark] = macroRW

  def fromJson(value: String): PushStreamWatermark = upickle.read(value)

  /** EPOCH serves as "null" value for the watermark, only used as a fallback
    * @return
    */
  def epoch: PushStreamWatermark = new PushStreamWatermark(
    timestamp = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)
  )
