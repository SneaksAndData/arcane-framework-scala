package com.sneaksanddata.arcane.framework
package services.base

import migrations.base.{CurrentMigration, Migration, SinkMigrationIdentifier}
import migrations.m1.{MbsEstimatesKeyMigration, WatermarkKeyMigration}
import models.schemas.ArcaneSchema

trait StreamingSource extends SchemaProvider[ArcaneSchema] with ShardProvider:

  /** Migrations defined for this streaming source.
    */
  def definedMigrations: Map[SinkMigrationIdentifier, Seq[Migration]] = Map(
    CurrentMigration -> Seq(
      new WatermarkKeyMigration(),
      new MbsEstimatesKeyMigration()
    )
  )
