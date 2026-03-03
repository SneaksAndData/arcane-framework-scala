package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.sink.AnalyzeSettings

object TestAnalyzeSettings extends AnalyzeSettings:
  override val batchThreshold: Int          = 10
  override val includedColumns: Seq[String] = Seq.empty[String]
