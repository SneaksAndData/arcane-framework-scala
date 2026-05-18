package com.sneaksanddata.arcane.framework
package models.queries

class ShardCommitQuery(targetName: String, sourceName: String) extends StreamingBatchQuery:
  override def query: String = s"INSERT INTO $targetName SELECT * FROM $sourceName"
