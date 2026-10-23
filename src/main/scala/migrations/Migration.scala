package com.sneaksanddata.arcane.framework
package migrations

import zio.Task

trait Migration:
  val shortName: String
  val description: String
  def execute: Task[Unit]
