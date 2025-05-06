package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.OrphanFilesExpirationSettings

object TestOrphanFilesExpirationSettings extends OrphanFilesExpirationSettings:
  val batchThreshold: Int = 10
  val retentionThreshold: String = "6h"
