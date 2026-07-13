package com.sneaksanddata.arcane.framework
package models.queries.backfill.mssql

import models.queries.StreamingBatchQuery

final class MsSqlShardResetQuery(shardTableName: String, combineTableName: String) extends StreamingBatchQuery:
  override def query: String = s"""INSERT INTO $combineTableName SELECT * FROM $shardTableName""".stripMargin
