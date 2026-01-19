package com.sneaksanddata.arcane.framework
package services.synapse.versioning

import services.streaming.base.SourceWatermark
import services.synapse.versioning.SynapseVersionExtensions.*

import java.time.OffsetDateTime

type SynapseVersionType = String

case class SynapseWatermark(version: SynapseVersionType, timestamp: OffsetDateTime, prefix: String) extends SourceWatermark[SynapseVersionType]:
  override def compare(that: SourceWatermark[SynapseVersionType]): Int = (version.asDate.toEpochSecond, that.version.asDate.toEpochSecond) match
    case (x, y) if x < y => -1
    case (x, y) if x == y => 0
    case (x, y) if x > y => 1
  
