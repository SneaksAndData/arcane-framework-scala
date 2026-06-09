package com.sneaksanddata.arcane.framework
package services.naming

import models.sharding.SourceShard

import zio.Task

/** Table name generator that ensures all names are bound to stream identifier
  */
trait NameGenerator:
  /** Generate name for a new staging table (streaming)
    */
  def getStagingTableName: Task[String]

  /** Get prefix used for all staging tables (streaming)
    */
  def getStagingTablePrefix: Task[String]

  /** Generate name for a new backfill table (overwrite and merge)
    */
  def getBackfillTableName: Task[String]

  /** Get prefix used for all staging tables (backfill)
    */
  def getBackfillTablesPrefix: Task[String]

  /** Generate name for the provided backfill source shard in the staging catalog
    */
  def getShardTableName(shard: SourceShard): Task[String]

  /** Generate name for the provided backfill source shard in the source
    */
  def getShardSourceTableName(shardId: String): Task[String]

  /** Generate target name (proxied from SinkSettings).
    */
  def getTargetTableName: Task[String]

  /** Generate target name for JDBC client.
    */
  def getTargetTableFullName: Task[String]
