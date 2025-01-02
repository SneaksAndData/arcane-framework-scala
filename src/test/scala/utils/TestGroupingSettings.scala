package com.sneaksanddata.arcane.framework
package utils

import models.settings.GroupingSettings

import java.time.Duration

class TestGroupingSettings(val groupingInterval: Duration, val rowsPerGroup: Int) extends GroupingSettings
