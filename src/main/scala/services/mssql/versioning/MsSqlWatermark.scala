package com.sneaksanddata.arcane.framework
package services.mssql.versioning

import services.streaming.base.SourceWatermark

import java.time.{Instant, OffsetDateTime, ZoneOffset}

type MsSqlVersionType = String

case class MsSqlWatermark(version: MsSqlVersionType, timestamp: OffsetDateTime)
    extends SourceWatermark[MsSqlVersionType]:
  override def compare(that: SourceWatermark[MsSqlVersionType]): Int = (version.toLong, that.version.toLong) match
    case (x, y) if x < y  => -1
    case (x, y) if x == y => 0
    case (x, y) if x > y  => 1

  def -(value: Int): Long = version.toLong - value

object MsSqlWatermark:
  def fromChangeTrackingVersion(version: Long, commitTime: OffsetDateTime): MsSqlWatermark = new MsSqlWatermark(
    version = version.toString,
    timestamp = commitTime
  )
