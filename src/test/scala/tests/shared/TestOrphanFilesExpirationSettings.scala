package com.sneaksanddata.arcane.framework
package tests.shared

import com.sneaksanddata.arcane.framework.models.settings.sink.OrphanFilesExpirationSettings

object TestOrphanFilesExpirationSettings extends OrphanFilesExpirationSettings:
  val batchThreshold: Int        = 10
  val retentionThreshold: String = "6h"
