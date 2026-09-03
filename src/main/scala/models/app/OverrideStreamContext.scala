package com.sneaksanddata.arcane.framework
package models.app

import models.settings.sources.OverrideStreamSourceSettings
import models.settings.streaming.OverrideStreamModeSettings

/** The stream mode that can be overridden by the stream override provided by the arcane operator with the environment
  * variable.
  */
trait OverrideStreamContext:
  val streamMode: Option[OverrideStreamModeSettings]

  val source: Option[OverrideStreamSourceSettings]
