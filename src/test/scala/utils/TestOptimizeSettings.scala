package com.sneaksanddata.arcane.framework
package utils

import models.settings.OptimizeSettings

object TestOptimizeSettings extends OptimizeSettings:
  val batchThreshold: Int = 10
  val fileSizeThreshold: String = "1GB"
