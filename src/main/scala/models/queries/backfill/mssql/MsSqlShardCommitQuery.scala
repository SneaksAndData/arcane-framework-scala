package com.sneaksanddata.arcane.framework
package models.queries.backfill.mssql

import models.queries.{OverwriteQuery, OverwriteReplaceQuery}
import models.settings.EmptyTablePropertiesSettings

type MsSqlShardCommitQuery = OverwriteReplaceQuery

object MsSqlShardCommitQuery:
  def apply(targetName: String, combineTableName: String): OverwriteQuery = OverwriteReplaceQuery(
    sourceQuery = s"""SELECT * FROM $combineTableName""".stripMargin,
    targetName = targetName,
    tablePropertiesSettings = EmptyTablePropertiesSettings
  )
