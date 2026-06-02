package com.sneaksanddata.arcane.framework
package services.naming

import models.settings.{BackfillIdentifier, StreamIdentifier}

import zio.Task

trait NameGenerator:
  def getStagingTableName: Task[String]
  def getBackfillTableName: Task[String]
  def getBackfillTablesPrefix: Task[String]
  def getShardTableName: Task[String]
  def getTargetTableName: Task[String]
  def getTargetTableFullName: Task[String]
