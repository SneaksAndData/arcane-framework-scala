package com.sneaksanddata.arcane.framework
package models.queries.backfill.blob

import models.queries.{OverwriteQuery, OverwriteReplaceQuery}
import models.schemas.MergeKeyField
import models.settings.EmptyTablePropertiesSettings

type BlobShardCommitQuery = OverwriteReplaceQuery

object BlobShardCommitQuery:
  // Deduplicate using PK and `createdon`, hence choosing lates version of each PK value
  def apply(targetName: String, combineTableName: String): OverwriteQuery = OverwriteReplaceQuery(
    sourceQuery = s"""SELECT * FROM (
                                   | SELECT * FROM $combineTableName WHERE ORDER BY ROW_NUMBER() OVER (PARTITION BY ${MergeKeyField.name} ORDER BY createdon DESC) FETCH FIRST 1 ROWS WITH TIES
                                   |)""".stripMargin,
    targetName = targetName,
    tablePropertiesSettings = EmptyTablePropertiesSettings
  )
