package com.sneaksanddata.arcane.framework
package migrations.base

import migrations.m1.WatermarkKeyMigration

val definedMigrations: Map[SinkMigrationIdentifier, Seq[Migration]] = Map(
  CurrentMigration -> Seq(new WatermarkKeyMigration())
)
