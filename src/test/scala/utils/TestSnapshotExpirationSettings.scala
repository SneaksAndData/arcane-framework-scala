package com.sneaksanddata.arcane.framework
package utils

import models.settings.SnapshotExpirationSettings

object TestSnapshotExpirationSettings extends SnapshotExpirationSettings:
  val batchThreshold: Int = 10
  val retentionThreshold: String = "6h"
