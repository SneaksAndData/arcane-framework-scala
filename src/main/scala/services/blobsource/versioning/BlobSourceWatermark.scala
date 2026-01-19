package com.sneaksanddata.arcane.framework
package services.blobsource.versioning

import services.streaming.base.SourceWatermark

import java.time.{Instant, OffsetDateTime, ZoneOffset}

type BlobSourceVersionType = String

case class BlobSourceWatermark(version: BlobSourceVersionType, timestamp: OffsetDateTime)
    extends SourceWatermark[BlobSourceVersionType]:
  override def compare(that: SourceWatermark[BlobSourceVersionType]): Int = (version.toLong, that.version.toLong) match
    case (x, y) if x < y  => -1
    case (x, y) if x == y => 0
    case (x, y) if x > y  => 1

object BlobSourceWatermark:
  val epoch: BlobSourceWatermark =
    val start = OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)
    BlobSourceWatermark(version = start.toEpochSecond.toString, timestamp = start)

  def fromEpochSecond(value: Long): BlobSourceWatermark = BlobSourceWatermark(
    version = value.toString,
    timestamp = OffsetDateTime.ofInstant(Instant.ofEpochSecond(value), ZoneOffset.UTC)
  )
