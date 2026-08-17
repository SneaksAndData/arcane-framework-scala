package com.sneaksanddata.arcane.framework
package services.streaming.throughput

import upickle.{ReadWriter, macroRW}

import java.time.{Duration, OffsetDateTime}
import models.serialization.OffsetDateTimeRW.*

case class MemoryBoundShaperEstimation(
    recordCount: Long,
    physicalSize: Long,
    rowSize: Long,
    partitions: Int,
    lastUpdate: OffsetDateTime
) derives ReadWriter:
  def toJson: String                    = upickle.write(this)
  def isOutdated(maxAge: Long): Boolean = Duration.between(OffsetDateTime.now(), lastUpdate).getSeconds >= maxAge
  def asLogString: String =
    s"records: $recordCount, table size (bytes): $physicalSize, row size (bytes): $rowSize, partitions: $partitions"

object MemoryBoundShaperEstimation:
  def fromJson(value: String): MemoryBoundShaperEstimation = upickle.read(value)
