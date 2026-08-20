package com.sneaksanddata.arcane.framework
package tests.pullstream

import models.settings.sources.pullstream.DefaultPullStreamSourceSettings

import zio.test.*

/** Covers the path production actually uses: the plugin deserializes these settings straight from the stream
  * configuration document, so the payload flattening options are only reachable if they survive that round trip.
  */
object PullStreamSourceSettingsTests extends ZIOSpecDefault:

  private val requiredFields =
    """"pullIndexKey":"pk","pullIndexValue":"v","watermarkFieldName":"timestampUTC","region":"eu-central-1","tableName":"arcane-push-stream-tokens","endpoint":null"""

  def spec: Spec[Any, Any] = suite("PullStreamSourceSettings")(
    test("reads the payload flattening options from the stream configuration") {
      val settings = upickle.read[DefaultPullStreamSourceSettings](
        s"""{$requiredFields,"jsonPointerExpression":"/payload","jsonArrayPointers":{"/payload":{"id":"push_event_id"}}}"""
      )

      assertTrue(settings.jsonPointerExpression.contains("/payload"))
      && assertTrue(settings.jsonArrayPointers == Map("/payload" -> Map("id" -> "push_event_id")))
    },
    test("keeps configurations written before the flattening options were introduced readable") {
      val settings = upickle.read[DefaultPullStreamSourceSettings](s"""{$requiredFields}""")

      // absent options must default to "no flattening" rather than failing the stream on startup
      assertTrue(settings.jsonArrayPointers.isEmpty) && assertTrue(settings.jsonPointerExpression.isEmpty)
    }
  )
