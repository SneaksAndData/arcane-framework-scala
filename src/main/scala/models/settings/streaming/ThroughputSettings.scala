package com.sneaksanddata.arcane.framework
package models.settings.streaming

import models.serialization.JavaDurationRW.*

import upickle.ReadWriter
import upickle.default.*
import upickle.implicits.key

import java.time.Duration

/**
 * Marker for shaper implementations
 */
sealed trait ThroughputShaperImpl

/**
 * Settings for memory bound shaper implementation
 */
case class MemoryBound(
    meanStringTypeSizeEstimate: Int,
    meanObjectTypeSizeEstimate: Int,
    burstEstimateDivisionFactor: Int,
    rateEstimateDivisionFactor: Int,
    chunkCostScale: Int,
    chunkCostMax: Int,
    tableRowCountWeight: Double,
    tableSizeWeight: Double,
    tableSizeScaleFactor: Int
) extends ThroughputShaperImpl derives ReadWriter

/**
 * Settings for the static shaper implementation
 */
case class Static() extends ThroughputShaperImpl derives ReadWriter

case class ThroughputShaperImplSettings(
                                       memoryBound: Option[MemoryBound],
                                       static: Option[Static]
                                       ) derives ReadWriter:
  def resolveShaperImpl: ThroughputShaperImpl = memoryBound.getOrElse(static.getOrElse(Static()))

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
