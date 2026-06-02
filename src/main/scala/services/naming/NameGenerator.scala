package com.sneaksanddata.arcane.framework
package services.naming

import models.settings.{BackfillIdentifier, StreamIdentifier}
import models.sharding.SourceShard

import zio.Task

trait NameGenerator:
  def getStagingTableName: Task[String]
  def getBackfillTableName: Task[String]
  def getBackfillTablesPrefix: Task[String]
  def getShardTableName(shard: SourceShard): Task[String]
  def getTargetTableName: Task[String]
  def getTargetTableFullName: Task[String]
