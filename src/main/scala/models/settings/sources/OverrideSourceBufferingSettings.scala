package com.sneaksanddata.arcane.framework
package models.settings.sources

import upickle.default.*
import upickle.ReadWriter
import upickle.implicits.key

/** A partial override of `SourceBufferingSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideSourceBufferingSettings:
  /** Optional override for the buffering strategy.
    */
  val bufferingStrategySetting: Option[BufferingSettings]

  /** Optional override for whether buffering is enabled.
    */
  val bufferingEnabled: Option[Boolean]

/** Default implementation for `OverrideSourceBufferingSettings` using optional values.
  */
case class DefaultOverrideSourceBufferingSettings(
    @key("strategy") override val bufferingStrategySetting: Option[BufferingSettings] = None,
    @key("enabled") override val bufferingEnabled: Option[Boolean] = None
) extends OverrideSourceBufferingSettings derives ReadWriter
