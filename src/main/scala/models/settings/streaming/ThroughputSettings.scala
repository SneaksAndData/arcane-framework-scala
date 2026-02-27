package com.sneaksanddata.arcane.framework
package models.settings.streaming

import java.time.Duration

enum ThroughputShaperImpl:
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
