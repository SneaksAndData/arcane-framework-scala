package com.sneaksanddata.arcane.framework
package services.mssql.versioning

import services.streaming.base.{JsonWatermark, SourceWatermark}
import services.streaming.base.OffsetDateTimeRW.*
import upickle.default.*

import java.time.{Instant, OffsetDateTime, ZoneOffset}

type MsSqlVersionType = String

case class MsSqlWatermark(version: MsSqlVersionType, timestamp: OffsetDateTime)
    extends SourceWatermark[MsSqlVersionType]
    with JsonWatermark:
  override def compare(that: SourceWatermark[MsSqlVersionType]): Int = (version.toLong, that.version.toLong) match
    case (x, y) if x < y  => -1
    case (x, y) if x == y => 0
    case (x, y) if x > y  => 1

  override def toJson: String = upickle.write(this)

  def -(value: Int): Long = version.toLong - value

object MsSqlWatermark:
  def fromChangeTrackingVersion(version: Long, commitTime: OffsetDateTime): MsSqlWatermark = new MsSqlWatermark(
    version = version.toString,
    timestamp = commitTime
  )

  implicit val rw: ReadWriter[MsSqlWatermark] = macroRW

  def fromJson(value: String): MsSqlWatermark = upickle.read(value)

  /** EPOCH serves as "null" value for the watermark, only used as a fallback
    * @return
    */
  def epoch: MsSqlWatermark = new MsSqlWatermark(
    version = "1",
    timestamp = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)
  )
