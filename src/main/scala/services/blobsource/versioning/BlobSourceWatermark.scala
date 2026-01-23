package com.sneaksanddata.arcane.framework
package services.blobsource.versioning

import services.streaming.base.OffsetDateTimeRW.*
import services.streaming.base.{JsonWatermark, SourceWatermark}

import upickle.default.*

import java.time.{Instant, OffsetDateTime, ZoneOffset}

type BlobSourceVersionType = String

case class BlobSourceWatermark(version: BlobSourceVersionType, timestamp: OffsetDateTime)
    extends SourceWatermark[BlobSourceVersionType]
    with JsonWatermark:
  override def compare(that: SourceWatermark[BlobSourceVersionType]): Int = (version.toLong, that.version.toLong) match
    case (x, y) if x < y  => -1
    case (x, y) if x == y => 0
    case (x, y) if x > y  => 1

  override def toJson: String = upickle.write(this)

object BlobSourceWatermark:
  implicit val rw: ReadWriter[BlobSourceWatermark] = macroRW

  def fromJson(value: String): BlobSourceWatermark = upickle.read(value)
  
  val epoch: BlobSourceWatermark =
    val start = OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)
    BlobSourceWatermark(version = start.toEpochSecond.toString, timestamp = start)

  def fromEpochSecond(value: Long): BlobSourceWatermark = BlobSourceWatermark(
    version = value.toString,
    timestamp = OffsetDateTime.ofInstant(Instant.ofEpochSecond(value), ZoneOffset.UTC)
  )
