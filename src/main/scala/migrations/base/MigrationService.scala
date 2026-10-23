package com.sneaksanddata.arcane.framework
package migrations.base

import exceptions.FatalStreamFailExceptionWithCause
import logging.ZIOLogAnnotations.zlog
import models.settings.TableName

import zio.{Task, URIO, ZIO}

trait MigrationService:
  final def isCurrent(migrationId: SinkMigrationIdentifier): Boolean = migrationId == CurrentMigration

  final def migrate(migrations: Seq[Migration]): URIO[Nothing, Unit] = ZIO
    .foreach(migrations) { migration =>
      (for
        _ <- zlog("Running migration: %s (%s)", migration.uniqueId.toString, migration.shortName)
        _ <- migration.execute
        _ <- zlog("Successfully applied migration: %s (%s)", migration.uniqueId.toString, migration.shortName)
      yield ()).orDieWith(e => FatalStreamFailExceptionWithCause(s"Failed to execute migration: ${migration.uniqueId} (${migration.shortName})", e))
    }
    .flatMap(_ => ZIO.unit)

  def getSinkMigration(sink: TableName): Task[SinkMigrationIdentifier]

  def diff(fromId: Option[SinkMigrationIdentifier], toId: SinkMigrationIdentifier): Seq[Migration] = fromId match
    case None                         => definedMigrations(toId)
    case Some(value) if value == toId => Seq.empty
    case Some(value) if value > toId =>
      throw new IllegalArgumentException(
        s"Sink start migration identifier $value is greater than target migration identifier $toId"
      )
    case Some(value) =>
      val appliedMigrations = definedMigrations(value).map(_.uniqueId).toSet
      val fullMigrations    = definedMigrations(toId)

      fullMigrations.filterNot(fm => appliedMigrations.contains(fm.uniqueId))
