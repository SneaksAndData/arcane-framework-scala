package com.sneaksanddata.arcane.framework
package migrations.base

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

  def diff(fromId: Option[SinkMigrationIdentifier], toId: SinkMigrationIdentifier): Seq[Migration] = fromId match 
    case None => definedMigrations(toId)
    case Some(value) if value == toId => Seq.empty
    case Some(value) if value > toId => throw new IllegalArgumentException(s"Sink start migration identifier $value is greater than target migration identifier $toId")
    case Some(value) => 
      val appliedMigrations = definedMigrations(value).map(_.uniqueId).toSet
      val fullMigrations = definedMigrations(toId)

      fullMigrations.filterNot(fm => appliedMigrations.contains(fm.uniqueId))

