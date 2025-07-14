package com.sneaksanddata.arcane.framework
package models.batches

import models.schemas.ArcaneType.LongType
import models.schemas.Field

object BlobBatchCommons:
  val versionField: Field = Field(
    name = "createdon",
    fieldType = LongType
  )
