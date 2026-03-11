package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.sources.{BufferingStrategy, SourceBufferingSettings, Unbounded}

object TestSourceBufferingSettings extends SourceBufferingSettings:
  override val bufferingStrategy: BufferingStrategy = Unbounded()
  override val bufferingEnabled: Boolean            = true
