package com.sneaksanddata.arcane.framework
package services.migration.graph

import migrations.base.CurrentMigration
import services.migration.MigrationService

import zio.{ZIO, ZLayer}
import zio.stream.ZStream

/** Stream builder for running migrations
  */
final class DefaultMigrationGraphBuilder(migrationService: MigrationService) extends MigrationGraphBuilder:
  override type ProcessedBatch = Unit

  override def produce(): ZStream[Any, Throwable, ProcessedBatch] = ZStream
    .fromIterableZIO {
      for sinkMigration <- migrationService.getSinkMigration
      yield migrationService.diff(sinkMigration, CurrentMigration)
    }
    .rechunk(1)
    .mapZIO { migration => migrationService.execute(migration) }

object DefaultMigrationGraphBuilder:
  type Environment = MigrationService
  val layer =
    ZLayer {
      for migrationService <- ZIO.service[MigrationService]
      yield new DefaultMigrationGraphBuilder(
        migrationService
      )
    }
