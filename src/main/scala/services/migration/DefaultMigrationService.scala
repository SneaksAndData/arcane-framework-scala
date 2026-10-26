package com.sneaksanddata.arcane.framework
package services.migration

import exceptions.FatalStreamFailExceptionWithCause
import logging.ZIOLogAnnotations.zlog
import migrations.base.{CurrentMigration, Migration, SinkMigrationIdentifier, definedMigrations}
import models.MetadataKeys
import services.iceberg.base.SinkPropertyManager
import services.migration.MigrationService

import com.sneaksanddata.arcane.framework.models.app.PluginStreamContext
import com.sneaksanddata.arcane.framework.models.settings.TableNaming.parts
import zio.prelude.PartialOrdOps
import zio.{Task, URIO, ZIO}

class DefaultMigrationService(sinkPropertyManager: SinkPropertyManager, sinkTableName: String, context: PluginStreamContext) extends MigrationService:
  override def isCurrent(migrationId: SinkMigrationIdentifier): Boolean = migrationId == CurrentMigration

  override def getSinkMigration: Task[Option[SinkMigrationIdentifier]] = sinkPropertyManager
    .getProperty(
      sinkTableName.parts.name,
      MetadataKeys.getFrameworkKey(MetadataKeys.migrationKey)
    )
    .map(_.map(_.toLong))
    .orDieWith(e => FatalStreamFailExceptionWithCause("Invalid migration identifier", e))

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
