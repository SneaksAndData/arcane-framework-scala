package com.sneaksanddata.arcane.framework
package services.migration

import exceptions.FatalStreamFailExceptionWithCause
import logging.ZIOLogAnnotations.zlog
import migrations.base.{CurrentMigration, Migration, SinkMigrationIdentifier}
import models.MetadataKeys
import models.app.PluginStreamContext
import models.settings.TableNaming.parts
import services.base.{MergeServiceClient, StreamingSource}
import services.iceberg.base.SinkPropertyManager
import services.migration.MigrationService

import zio.{Task, ZIO, ZLayer}

class DefaultMigrationService(
    sinkPropertyManager: SinkPropertyManager,
    context: PluginStreamContext,
    mergeClient: MergeServiceClient,
    streamingSource: StreamingSource
) extends MigrationService:
  override def isCurrent(migrationId: SinkMigrationIdentifier): Boolean = migrationId == CurrentMigration

  override def getSinkMigration: Task[Option[SinkMigrationIdentifier]] = sinkPropertyManager
    .getProperty(
      context.sink.targetTableFullName.parts.name,
      MetadataKeys.getFrameworkKey(MetadataKeys.migrationKey)
    )
    .map(_.map(_.toLong))
    .orDieWith(e => FatalStreamFailExceptionWithCause("Invalid migration identifier", e))

  override def diff(fromId: Option[SinkMigrationIdentifier], toId: SinkMigrationIdentifier): Seq[Migration] =
    fromId match
      case None                         => streamingSource.definedMigrations(toId)
      case Some(value) if value == toId => Seq.empty
      case Some(value) if value > toId =>
        throw new IllegalArgumentException(
          s"Sink start migration identifier $value is greater than target migration identifier $toId"
        )
      case Some(value) =>
        val appliedMigrations = streamingSource.definedMigrations(value).map(_.uniqueId).toSet
        val fullMigrations    = streamingSource.definedMigrations(toId)

        fullMigrations.filterNot(fm => appliedMigrations.contains(fm.uniqueId))

  override def execute(migration: Migration): ZIO[Any, Throwable, Unit] = (for
    _ <- zlog("Running migration: %s (%s)", migration.uniqueId.toString, migration.shortName)
    _ <- migration.execute(sinkPropertyManager, context, mergeClient, streamingSource)
    _ <- zlog("Successfully applied migration: %s (%s)", migration.uniqueId.toString, migration.shortName)
  yield ()).orDieWith(e =>
    FatalStreamFailExceptionWithCause(
      s"Failed to execute migration: ${migration.uniqueId} (${migration.shortName})",
      e
    )
  )

object DefaultMigrationService:
  val layer = ZLayer {
    for
      sinkPropertyManager <- ZIO.service[SinkPropertyManager]
      context             <- ZIO.service[PluginStreamContext]
      mergeClient         <- ZIO.service[MergeServiceClient]
      streamingSource     <- ZIO.service[StreamingSource]
    yield new DefaultMigrationService(sinkPropertyManager, context, mergeClient, streamingSource)
  }
