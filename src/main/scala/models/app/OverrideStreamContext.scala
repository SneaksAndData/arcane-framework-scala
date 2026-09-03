package com.sneaksanddata.arcane.framework
package models.app

import models.settings.streaming.{DefaultOverrideStreamModeSettings, OverrideStreamModeSettings}

import upickle.ReadWriter
import upickle.implicits.key

/** The stream mode that can be overridden by the stream override provided by the arcane operator with the environment
  * variable.
  */
trait OverrideStreamContext:
  val streamMode: Option[OverrideStreamModeSettings]

/** The stream mode that can be overridden by the stream override provided by the arcane operator with the environment
  * variable.
  */
case class DefaultOverrideStreamContext(
    @key("streamMode") override val streamMode: Option[DefaultOverrideStreamModeSettings] = None
) extends OverrideStreamContext derives ReadWriter
