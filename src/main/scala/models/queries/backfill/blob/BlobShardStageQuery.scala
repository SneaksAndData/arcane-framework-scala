package com.sneaksanddata.arcane.framework
package models.queries.backfill.blob

import models.queries.StreamingBatchQuery

final class BlobShardStageQuery(shardTableName: String, combineTableName: String) extends StreamingBatchQuery:
  override def query: String = s"""INSERT INTO $combineTableName SELECT * FROM $shardTableName""".stripMargin
