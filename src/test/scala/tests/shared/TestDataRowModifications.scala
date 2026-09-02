package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.sources.modification.*

object TestDataRowModifications:
  val mergeModifications: Seq[DataRowModification] = Seq(
    SurrogateMergeKeyImpl(SurrogateMergeKey()),
    SurrogateVersionImpl(SurrogateVersion())
  )
