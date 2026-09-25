package com.sneaksanddata.arcane.framework
package models.queries.backfill.blob

import models.queries.{OverwriteQuery, OverwriteReplaceQuery}
import models.schemas.{MergeKeyField, VersionField}
import models.settings.EmptyTablePropertiesSettings

type BlobShardCommitQuery = OverwriteReplaceQuery

object BlobShardCommitQuery:
  // Deduplicate using the merge key and Arcane version, choosing the latest version of each key.
  def apply(targetName: String, combineTableName: String): OverwriteQuery = OverwriteReplaceQuery(
    sourceQuery = s"""SELECT * FROM (
                                   | SELECT * FROM $combineTableName ORDER BY ROW_NUMBER() OVER (PARTITION BY ${MergeKeyField.name} ORDER BY ${VersionField.name} DESC) FETCH FIRST 1 ROWS WITH TIES
                                   |)""".stripMargin,
    targetName = targetName,
    tablePropertiesSettings = EmptyTablePropertiesSettings
  )
