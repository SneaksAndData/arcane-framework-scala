package com.sneaksanddata.arcane.framework
package services.migration

import exceptions.FatalStreamFailExceptionWithCause
import logging.ZIOLogAnnotations.zlog
import migrations.base.{Migration, SinkMigrationIdentifier}

import zio.{Task, ZIO}

trait MigrationService:
  def isCurrent(migrationId: SinkMigrationIdentifier): Boolean

  def getSinkMigration: Task[Option[SinkMigrationIdentifier]]

  def diff(fromId: Option[SinkMigrationIdentifier], toId: SinkMigrationIdentifier): Seq[Migration]

  /** Returns the migrations that still need to run, skipping those already applied by a previous migration run that did
    * not complete.
    */
  def getPendingMigrations(migrations: Seq[Migration]): Task[Seq[Migration]]

  def execute(migration: Migration): ZIO[Any, Throwable, Unit]

  def commitMigration: Task[Unit]
