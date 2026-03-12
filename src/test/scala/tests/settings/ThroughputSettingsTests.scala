package com.sneaksanddata.arcane.framework
package tests.settings

import models.settings.streaming.{DefaultThroughputSettings, MemoryBound, Static, ThroughputShaperImplSettings}

import org.scalatest.Inspectors.forAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import upickle.default.*

import java.time.Duration

class ThroughputSettingsTests extends AnyFlatSpec with Matchers:

  private val testCases = List(
    (
      DefaultThroughputSettings(
        ThroughputShaperImplSettings(
          memoryBound = Some(
            MemoryBound(
              meanStringTypeSizeEstimate = 1,
              meanObjectTypeSizeEstimate = 1,
              burstEstimateDivisionFactor = 1,
              rateEstimateDivisionFactor = 1,
              chunkCostScale = 1,
              tableRowCountWeight = 1,
              tableSizeWeight = 1,
              tableSizeScaleFactor = 1
            )
          ),
          static = None
        ),
        advisedRatePeriod = Duration.ofSeconds(1),
        advisedChunkSize = 1,
        advisedRateChunks = 1,
        advisedChunksBurst = 1
      ),
      """{"shaperImpl":{"memoryBound":{"meanStringTypeSizeEstimate":1,"meanObjectTypeSizeEstimate":1,"burstEstimateDivisionFactor":1,"rateEstimateDivisionFactor":1,"chunkCostScale":1,"tableRowCountWeight":1,"tableSizeWeight":1,"tableSizeScaleFactor":1},"static":null},"advisedRatePeriod":"1 second","advisedChunksBurst":1,"advisedChunkSize":1,"advisedRateChunks":1}"""
    ),
    (
      DefaultThroughputSettings(
        ThroughputShaperImplSettings(
          memoryBound = None,
          static = Some(Static())
        ),
        advisedRatePeriod = Duration.ofSeconds(1),
        advisedChunkSize = 1,
        advisedRateChunks = 1,
        advisedChunksBurst = 1
      ),
      """{"shaperImpl":{"memoryBound":null,"static":{}},"advisedRatePeriod":"1 second","advisedChunksBurst":1,"advisedChunkSize":1,"advisedRateChunks":1}"""
    )
  )

  it should "serialize correctly" in {
    forAll(testCases) { (settings, expected) =>
      upickle.write(settings) should equal(expected)
    }
  }

  it should "deserialize correctly" in {
    forAll(testCases) { (settings, expected) =>
      read[DefaultThroughputSettings](expected) should equal(settings)
    }
  }
