package com.sneaksanddata.arcane.framework
package utils

import models.settings.{SourceBufferingSettings, BufferingStrategy}

object TestSourceBufferingSettings extends SourceBufferingSettings:
  override val bufferingStrategy: BufferingStrategy = BufferingStrategy.Unbounded
  override val bufferingEnabled: Boolean = true
