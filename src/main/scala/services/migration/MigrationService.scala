package com.sneaksanddata.arcane.framework
package services.migration

import exceptions.FatalStreamFailExceptionWithCause
import logging.ZIOLogAnnotations.zlog
import migrations.base.{Migration, SinkMigrationIdentifier}
import models.settings.TableName

import zio.{Task, URIO, ZIO}

trait MigrationService:
  def isCurrent(migrationId: SinkMigrationIdentifier): Boolean

  def migrate(migrations: Seq[Migration]): URIO[Nothing, Unit]

  def getSinkMigration(tableName: String): Task[Option[SinkMigrationIdentifier]]

  def diff(fromId: Option[SinkMigrationIdentifier], toId: SinkMigrationIdentifier): Seq[Migration]
