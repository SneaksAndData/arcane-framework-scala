package com.sneaksanddata.arcane.framework
package tests.settings

import models.settings.sources.{Buffering, BufferingSettings, DefaultSourceBufferingSettings, Unbounded}

import org.scalatest.Inspectors.forAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import upickle.default.*

class SourceBufferingSettingsTests extends AnyFlatSpec with Matchers:

  private val testCases = List(
    (
      DefaultSourceBufferingSettings(
        BufferingSettings(
          unbounded = None,
          buffered = Some(Buffering(10000))
        ),
        bufferingEnabled = true
      ),
      """{"strategy":{"buffered":{"maxBufferSize":10000}},"enabled":true}"""
    ),
    (
      DefaultSourceBufferingSettings(
        BufferingSettings(
          unbounded = Some(Unbounded()),
          buffered = None
        ),
        bufferingEnabled = true
      ),
      """{"strategy":{"unbounded":{}},"enabled":true}"""
    ),
    (
      DefaultSourceBufferingSettings(
        BufferingSettings(
          unbounded = None,
          buffered = None
        ),
        bufferingEnabled = false
      ),
      """{"strategy":{},"enabled":false}"""
    )
  )

  it should "serialize correctly" in {
    forAll(testCases) { (settings, expected) =>
      upickle.write(settings) should equal(expected)
    }
  }

  it should "deserialize correctly" in {
    forAll(testCases) { (settings, expected) =>
      read[DefaultSourceBufferingSettings](expected) should equal(settings)
    }
  }
