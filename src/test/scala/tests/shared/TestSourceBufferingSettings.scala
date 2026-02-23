package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.sources.{BufferingStrategy, SourceBufferingSettings}

object TestSourceBufferingSettings extends SourceBufferingSettings:
  override val bufferingStrategy: BufferingStrategy = BufferingStrategy.Unbounded
  override val bufferingEnabled: Boolean            = true
