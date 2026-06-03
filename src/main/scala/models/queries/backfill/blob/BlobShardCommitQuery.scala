package com.sneaksanddata.arcane.framework
package models.queries.backfill.blob

import models.queries.{OverwriteQuery, OverwriteReplaceQuery}
import models.settings.EmptyTablePropertiesSettings

type BlobShardCommitQuery = OverwriteReplaceQuery

object BlobShardCommitQuery:
  def apply(targetName: String, combineTableName: String): OverwriteQuery = OverwriteReplaceQuery(
    sourceQuery = s"""SELECT * FROM $combineTableName""".stripMargin,
    targetName = targetName,
    tablePropertiesSettings = EmptyTablePropertiesSettings
  )
