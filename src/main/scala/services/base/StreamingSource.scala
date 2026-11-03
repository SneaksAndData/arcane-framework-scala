package com.sneaksanddata.arcane.framework
package services.base

import migrations.base.{CurrentMigration, Migration, SinkMigrationIdentifier}
import migrations.m1.{
  FieldSelectorSchemaMigration,
  MbsEstimatesKeyMigration,
  TableCommentMigration,
  WatermarkKeyMigration
}
import models.schemas.ArcaneSchema

trait StreamingSource extends SchemaProvider[ArcaneSchema] with ShardProvider:

  /** Fields that must always be preserved in the sink schema even when field selectors are configured.
    */
  def essentialFields: Set[String] = Set.empty

  /** Migrations defined for this streaming source.
    */
  def definedMigrations: Map[SinkMigrationIdentifier, Seq[Migration]] = Map(
    CurrentMigration -> Seq(
      new WatermarkKeyMigration(),
      new MbsEstimatesKeyMigration(),
      new TableCommentMigration()
    )
  )
