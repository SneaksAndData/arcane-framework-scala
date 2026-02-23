package com.sneaksanddata.arcane.framework
package tests.shared

import com.sneaksanddata.arcane.framework.models.settings.sink.OptimizeSettings

object TestOptimizeSettings extends OptimizeSettings:
  val batchThreshold: Int       = 10
  val fileSizeThreshold: String = "1GB"
