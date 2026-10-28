package com.sneaksanddata.arcane.framework
package tests.pullstream

import models.settings.sources.pullstream.DefaultPullStreamSourceSettings

import zio.test.*

import scala.util.Try

/** Covers the path production actually uses: the plugin deserializes these settings straight from the stream
  * configuration document, so the payload pointer option is only reachable if it survives that round trip.
  */
object PullStreamSourceSettingsTests extends ZIOSpecDefault:

  private val requiredFields =
    """"pullIndexKey":"pk","pullIndexValue":"v","versionFieldName":"timestampUTC","region":"eu-central-1","tableName":"arcane-push-stream-tokens","endpoint":null"""

  def spec: Spec[Any, Any] = suite("PullStreamSourceSettings")(
    test("deserializes default pull stream source settings") {
      val settings = upickle.read[DefaultPullStreamSourceSettings](
        s"""{$requiredFields}"""
      )

      assertTrue(
        settings.pullIndexKey == "pk",
        settings.pullIndexValue == "v",
        settings.versionFieldName == "timestampUTC",
        settings.region == "eu-central-1",
        settings.tableName == "arcane-push-stream-tokens",
        settings.endpoint.isEmpty
      )
    }
  )
