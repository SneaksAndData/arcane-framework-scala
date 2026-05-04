package com.sneaksanddata.arcane.framework
package tests.settings

import models.settings.FlowRate
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
              fallbackStringTypeSizeEstimate = 1,
              objectTypeSizeEstimate = 1,
              chunkCostScale = 1,
              chunkCostMax = 1,
              tableRowCountWeight = 1,
              tableSizeWeight = 1,
              tableSizeScaleFactor = 1
            )
          ),
          static = None
        ),
        advisedChunkSize = 1,
        advisedRate = FlowRate(elements = 1, interval = Duration.ofSeconds(5)),
        advisedBurst = 1
      ),
      """{"shaperImpl":{"memoryBound":{"fallbackStringTypeSizeEstimate":1,"objectTypeSizeEstimate":1,"chunkCostScale":1,"chunkCostMax":1,"tableRowCountWeight":1,"tableSizeWeight":1,"tableSizeScaleFactor":1}},"advisedRate":"1 per 5 seconds","advisedBurst":1,"advisedChunkSize":1}"""
    ),
    (
      DefaultThroughputSettings(
        ThroughputShaperImplSettings(
          memoryBound = None,
          static = Some(Static())
        ),
        advisedRate = FlowRate(elements = 1, interval = Duration.ofSeconds(5)),
        advisedChunkSize = 1,
        advisedBurst = 1
      ),
      """{"shaperImpl":{"static":{}},"advisedRate":"1 per 5 seconds","advisedBurst":1,"advisedChunkSize":1}"""
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
