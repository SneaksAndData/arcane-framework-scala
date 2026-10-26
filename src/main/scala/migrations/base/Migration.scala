package com.sneaksanddata.arcane.framework
package migrations.base

import models.app.PluginStreamContext
import services.iceberg.base.SinkPropertyManager
import services.merging.JdbcMergeServiceClient

import zio.ZIO

trait Migration:
  val shortName: String
  val description: String
  val uniqueId: Long

  def execute(
      sinkPropertyManager: SinkPropertyManager,
      context: PluginStreamContext,
      mergeClient: JdbcMergeServiceClient
  ): ZIO[Any, Throwable, Unit]
