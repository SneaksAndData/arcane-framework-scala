package com.sneaksanddata.arcane.framework
package services.migration.graph

import com.sneaksanddata.arcane.framework.migrations.base.CurrentMigration
import com.sneaksanddata.arcane.framework.services.migration.MigrationService
import zio.{ZIO, ZLayer}
import zio.stream.ZStream

final class DefaultMigrationGraphBuilder(migrationService: MigrationService) extends MigrationGraphBuilder:
  override type ProcessedBatch = Unit

  override def produce(): ZStream[Any, Throwable, ProcessedBatch] = ZStream.fromZIO {
    for
      sinkMigration <- migrationService.getSinkMigration
    yield migrationService.diff(sinkMigration, CurrentMigration)
  }.flatMap(ZStream.fromIterable).mapZIO { migration =>
    for
      _ <- migration.execute.provide(ZLayer.succeed(services))
    yield ()
  }