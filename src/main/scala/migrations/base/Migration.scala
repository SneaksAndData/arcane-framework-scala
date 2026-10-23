package com.sneaksanddata.arcane.framework
package migrations.base

import zio.Task

trait Migration:
  val shortName: String
  val description: String
  val uniqueId: Long
  
  def execute: Task[Unit]
