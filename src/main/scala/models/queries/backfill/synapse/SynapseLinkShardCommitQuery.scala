package com.sneaksanddata.arcane.framework
package models.queries.backfill.synapse

import models.queries.{OverwriteQuery, OverwriteReplaceQuery}
import models.schemas.MergeKeyField
import models.settings.EmptyTablePropertiesSettings

type SynapseLinkShardCommitQuery = OverwriteReplaceQuery

object SynapseLinkShardCommitQuery:
  def apply(targetName: String, combineTableName: String): OverwriteQuery = OverwriteReplaceQuery(
    sourceQuery = s"""SELECT * FROM (
                     | SELECT * FROM $combineTableName WHERE ORDER BY ROW_NUMBER() OVER (PARTITION BY ${MergeKeyField.name} ORDER BY versionnumber DESC) FETCH FIRST 1 ROWS WITH TIES
                     |)""".stripMargin,
    targetName = targetName,
    tablePropertiesSettings = EmptyTablePropertiesSettings
  )
