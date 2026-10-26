package com.sneaksanddata.arcane.framework
package services.migration.graph

import com.sneaksanddata.arcane.framework.exceptions.FatalStreamFailExceptionWithCause
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
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
  }.flatMap(ZStream.fromIterable).rechunk(1).mapZIO { migration =>
    for
        _ <- zlog("Running migration: %s (%s)", migration.uniqueId.toString, migration.shortName)
        _ <- ZIO.environment[migration.RequiredMigrationServices].flatMap(env => migration.execute.provideEnvironment(env))
        _ <- zlog("Successfully applied migration: %s (%s)", migration.uniqueId.toString, migration.shortName)
    yield () 
  }


//.orDieWith(e =>
//  FatalStreamFailExceptionWithCause(
//    s"Failed to execute migration: ${migration.uniqueId} (${migration.shortName})",
//    e
//  ))