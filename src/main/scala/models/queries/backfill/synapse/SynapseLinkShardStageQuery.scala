package com.sneaksanddata.arcane.framework
package models.queries.backfill.synapse

import models.queries.StreamingBatchQuery
import models.schemas.{MergeKeyField, VersionField}

final class SynapseLinkShardStageQuery(shardTableName: String, combineTableName: String) extends StreamingBatchQuery:
  override def query: String = s"""INSERT INTO $combineTableName 
                                  |SELECT * FROM (SELECT * FROM $shardTableName WHERE COALESCE(IsDelete, false) = false ORDER BY ROW_NUMBER() OVER (PARTITION BY ${MergeKeyField.name} ORDER BY ${VersionField.name} DESC) FETCH FIRST 1 ROWS WITH TIES)""".stripMargin
