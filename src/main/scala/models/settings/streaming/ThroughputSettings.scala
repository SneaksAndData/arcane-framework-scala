package com.sneaksanddata.arcane.framework
package models.settings.streaming

import models.serialization.FlowRateRW.*
import models.settings.FlowRate

import upickle.default.*
import upickle.implicits.key

/** Marker for shaper implementations
  */
sealed trait ThroughputShaperImpl

/** Settings for memory bound shaper implementation
  */
case class MemoryBound(
    fallbackStringTypeSizeEstimate: Int,
    objectTypeSizeEstimate: Int,
    chunkCostScale: Int,
    chunkCostMax: Int,
    tableRowCountWeight: Double,
    tableSizeWeight: Double,
    tableSizeScaleFactor: Int
) derives ReadWriter

/** ADT composed with settings class for MemoryBound
  */
case class MemoryBoundImpl(memoryBound: MemoryBound) extends ThroughputShaperImpl

/** Settings for the static shaper implementation
  */
case class Static() derives ReadWriter

/** ADT composed with settings class for Static
  */
case class StaticImpl(static: Static) extends ThroughputShaperImpl

case class ThroughputShaperImplSettings(
    memoryBound: Option[MemoryBound] = None,
    static: Option[Static] = None
) derives ReadWriter:
  def resolveShaperImpl: ThroughputShaperImpl = memoryBound
    .map(MemoryBoundImpl(_))
    .getOrElse(
      static
        .map(StaticImpl(_))
        .getOrElse(StaticImpl(Static()))
    )

trait ThroughputSettings:
  val shaperImpl: ThroughputShaperImpl

  val advisedChunkSize: Int
  val advisedRate: FlowRate
  val advisedBurst: Int

case class DefaultThroughputSettings(
    @key("shaperImpl") shaperImplSetting: ThroughputShaperImplSettings,
    override val advisedRate: FlowRate,
    override val advisedBurst: Int,
    override val advisedChunkSize: Int
) extends ThroughputSettings derives ReadWriter:
  override val shaperImpl: ThroughputShaperImpl = shaperImplSetting.resolveShaperImpl
