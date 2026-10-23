package com.sneaksanddata.arcane.framework
package migrations.m1

import migrations.base.Migration

case object WatermarkKeyMigration extends Migration:
  val description: String = "Ensures watermark value is stored under arcane.core.watermark property"

  def execute: zio.Task[Unit] = ???

  val shortName: String = "watermark_key"
  val uniqueId: Long = 61492573L

