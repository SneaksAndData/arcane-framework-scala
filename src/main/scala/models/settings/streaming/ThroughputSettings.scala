package com.sneaksanddata.arcane.framework
package models.settings.streaming

import models.serialization.JavaDurationRW.*

import upickle.default.*
import upickle.implicits.key

import java.time.Duration

/** Marker for shaper implementations
  */
sealed trait ThroughputShaperImpl

/** Settings for memory bound shaper implementation
  */
case class MemoryBound(
    fallbackStringTypeSizeEstimate: Int,
    meanObjectTypeSizeEstimate: Int,
    burstEstimateDivisionFactor: Int,
    rateEstimateDivisionFactor: Int,
    chunkCostScale: Int,
    chunkCostMax: Int,
    tableRowCountWeight: Double,
    tableSizeWeight: Double,
    tableSizeScaleFactor: Int,
    dataCompressionFactor: Double
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
  val advisedRateChunks: Int
  val advisedRatePeriod: Duration
  val advisedChunksBurst: Int

case class DefaultThroughputSettings(
    @key("shaperImpl") shaperImplSetting: ThroughputShaperImplSettings,
    override val advisedRatePeriod: Duration,
    override val advisedChunksBurst: Int,
    override val advisedChunkSize: Int,
    override val advisedRateChunks: Int
) extends ThroughputSettings derives ReadWriter:
  override val shaperImpl: ThroughputShaperImpl = shaperImplSetting.resolveShaperImpl
