package com.sneaksanddata.arcane.framework
package models.queries

/**
 * Commit query to use with shards by default. 
 * Note that some shard might require data filtering or aggregation before they can be committed - then you should define a custom query.
 */
case class DefaultShardCommitQuery(targetName: String, sourceName: String) extends StreamingBatchQuery:
  override def query: String = s"INSERT INTO $targetName SELECT * FROM $sourceName"
