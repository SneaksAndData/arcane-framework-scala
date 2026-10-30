package com.sneaksanddata.arcane.framework
package services.migration

import exceptions.FatalStreamFailExceptionWithCause
import logging.ZIOLogAnnotations.zlog
import migrations.base.{Migration, SinkMigrationIdentifier}
import models.settings.TableName

import zio.{Task, UIO, URIO, ZIO}

trait MigrationService:
  def isCurrent(migrationId: SinkMigrationIdentifier): Boolean

  def getSinkMigration: Task[Option[SinkMigrationIdentifier]]

  def diff(fromId: Option[SinkMigrationIdentifier], toId: SinkMigrationIdentifier): Seq[Migration]

  def execute(migration: Migration): ZIO[Any, Throwable, Unit]

  def commitMigration: Task[Unit]
