package com.sneaksanddata.arcane.framework
package services.migration.graph

import migrations.base.CurrentMigration
import services.migration.MigrationService

import zio.stream.ZStream

final class DefaultMigrationGraphBuilder(migrationService: MigrationService) extends MigrationGraphBuilder:
  override type ProcessedBatch = Unit

  override def produce(): ZStream[Any, Throwable, ProcessedBatch] = ZStream.fromIterableZIO {
    for
      sinkMigration <- migrationService.getSinkMigration
    yield migrationService.diff(sinkMigration, CurrentMigration)
  }.rechunk(1).mapZIO { migration => migrationService.execute(migration) }
