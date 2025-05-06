package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.GroupingSettings

import java.time.Duration

object TestGroupingSettings extends GroupingSettings:
  override val groupingInterval: Duration = Duration.ofMillis(1)
  override val rowsPerGroup: Int = 1
