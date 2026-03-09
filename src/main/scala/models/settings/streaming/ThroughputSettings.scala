package com.sneaksanddata.arcane.framework
package models.settings.streaming

import upickle.ReadWriter
import upickle.default.*

import java.time.Duration

enum ThroughputShaperImpl derives ReadWriter:
  case MemoryBound(
      meanStringTypeSizeEstimate: Int,
      meanObjectTypeSizeEstimate: Int,
      burstEstimateDivisionFactor: Int,
      rateEstimateDivisionFactor: Int,
      chunkCostScale: Int,
      chunkCostMax: Int,
      tableRowCountWeight: Double,
      tableSizeWeight: Double,
      tableSizeScaleFactor: Int
  )
  case Static

trait ThroughputSettings:
  val shaperImpl: ThroughputShaperImpl

  val advisedChunkSize: Int
  val advisedRateChunks: Int
  val advisedRatePeriod: Duration
  val advisedChunksBurst: Int

case class DefaultThroughputSettings(
                                      override val shaperImpl: ThroughputShaperImpl,
                                      override val advisedRatePeriod: Duration,
                                      override val advisedChunksBurst: Int,
                                      override val advisedChunkSize: Int,
                                      override val advisedRateChunks: Int
                                    ) extends ThroughputSettings derives ReadWriter
