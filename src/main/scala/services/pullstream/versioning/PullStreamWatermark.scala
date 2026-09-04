package com.sneaksanddata.arcane.framework
package services.pullstream.versioning

import models.serialization.OffsetDateTimeRW.*
import services.streaming.base.{JsonWatermark, SourceWatermark}

import upickle.default.*

import java.time.{Instant, OffsetDateTime, ZoneOffset}

type PullStreamVersionType = String

/** PullStreamWatermark Watermark is a single timestamp.
  */
case class PullStreamWatermark(timestamp: OffsetDateTime)
    extends SourceWatermark[PullStreamVersionType]
    with JsonWatermark:
  override def compare(that: SourceWatermark[PullStreamVersionType]): Int =
    Ordering[Long].compare(version.toLong, that.version.toLong)

  override def toJson: String = upickle.write(this)

  /** Current source version associated with this watermark
    */
  override val version: PullStreamVersionType = timestamp.toEpochSecond.toString

object PullStreamWatermark:
  def fromChangeTrackingVersion(commitTime: OffsetDateTime): PullStreamWatermark = new PullStreamWatermark(
    timestamp = commitTime
  )

  implicit val rw: ReadWriter[PullStreamWatermark] = macroRW

  def fromJson(value: String): PullStreamWatermark = upickle.read(value)

  /** EPOCH serves as "null" value for the watermark, only used as a fallback
    * @return
    */
  def epoch: PullStreamWatermark = new PullStreamWatermark(
    timestamp = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)
  )
