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
    """"pullIndexKey":"pk","pullIndexValue":"v","versionFieldName":"timestampUTC","region":"eu-central-1","tableName":"arcane-push-stream-tokens","endpoint":null,"pageSize":null"""

  def spec: Spec[Any, Any] = suite("PullStreamSourceSettings")(
    test("reads the payload pointer option from the stream configuration") {
      val settings = upickle.read[DefaultPullStreamSourceSettings](
        s"""{$requiredFields,"jsonPointerExpression":"/payload"}"""
      )

      assertTrue(settings.jsonPointerExpression.contains("/payload"))
    },
    test("reads a configuration that decodes the document from its root") {
      val settings = upickle.read[DefaultPullStreamSourceSettings](
        s"""{$requiredFields,"jsonPointerExpression":null}"""
      )

      assertTrue(settings.jsonPointerExpression.isEmpty)
    },
    test("rejects a configuration that omits an optional-valued key") {
      // every field is mandatory in the document even when its value is nullable, so a configuration written before
      // these keys existed fails loudly on startup instead of silently streaming with an unintended default
      val parsed = Try(upickle.read[DefaultPullStreamSourceSettings](s"""{$requiredFields}"""))

      assertTrue(parsed.isFailure)
    }
  )
