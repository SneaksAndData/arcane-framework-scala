package com.sneaksanddata.arcane.framework
package migrations.base

import models.app.PluginStreamContext
import services.iceberg.base.SinkPropertyManager

import zio.{Task, ZIO}

trait Migration:
  val shortName: String
  val description: String
  val uniqueId: Long

  def execute(sinkPropertyManager: SinkPropertyManager, context: PluginStreamContext): ZIO[Any, Throwable, Unit]
