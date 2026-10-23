package com.sneaksanddata.arcane.framework
package migrations

import exceptions.FatalStreamFailExceptionWithCause
import models.settings.TableName

import zio.{Task, ZIO}

trait MigrationService:
  final def isCurrent(migrationId: SinkMigrationIdentifier): Boolean = migrationId == CurrentMigration

  final def migrate(migrations: Seq[Migration]): ZIO[Any, Throwable, Unit] = ZIO
    .foreach(migrations)(_.execute)
    .flatMap(_ => ZIO.unit)
    .orDieWith(e =>
      FatalStreamFailExceptionWithCause(
        s"Failed to execute provided migrations: ${migrations.map(_.shortName).mkString(",")}",
        e
      )
    )

  def getSinkMigration(sink: TableName): Task[SinkMigrationIdentifier]

  def diff(fromId: SinkMigrationIdentifier, toId: SinkMigrationIdentifier): Task[Seq[Migration]]
