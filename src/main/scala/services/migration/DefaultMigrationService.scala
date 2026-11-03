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
  private val targetTableName = context.sink.targetTableFullName.parts.name

  private val appliedMigrationsProperty = MetadataKeys.getFrameworkKey(MetadataKeys.appliedMigrationsKey)

  override def isCurrent(migrationId: SinkMigrationIdentifier): Boolean = migrationId == CurrentMigration

  override def getSinkMigration: Task[Option[SinkMigrationIdentifier]] = sinkPropertyManager
    .getProperty(
      targetTableName,
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

  override def getPendingMigrations(migrations: Seq[Migration]): Task[Seq[Migration]] = for
    applied <- getAppliedMigrations
    (skipped, pending) = migrations.partition(m => applied.contains(m.uniqueId))
    _ <- ZIO.foreachDiscard(skipped) { migration =>
      zlog(
        "Skipping migration already applied by an unfinished migration run: %s (%s)",
        migration.uniqueId.toString,
        migration.shortName
      )
    }
  yield pending

  /** Reads identifiers of migrations already applied by the current, not yet committed, migration run.
    */
  private def getAppliedMigrations: Task[Set[SinkMigrationIdentifier]] = sinkPropertyManager
    .getProperty(targetTableName, appliedMigrationsProperty)
    .map(_.map(_.split(',').map(_.toLong).toSet).getOrElse(Set.empty))
    .orDieWith(e => FatalStreamFailExceptionWithCause("Invalid applied migrations identifier", e))

  /** Checkpoints a successfully applied migration so it is skipped if a later migration in the same run fails.
    */
  private def recordAppliedMigration(migration: Migration): Task[Unit] = for
    applied <- getAppliedMigrations
    _ <- sinkPropertyManager.setProperty(
      targetTableName,
      appliedMigrationsProperty,
      (applied + migration.uniqueId).toSeq.sorted.mkString(",")
    )
  yield ()

  override def execute(migration: Migration): ZIO[Any, Throwable, Unit] = (for
    _ <- zlog("Running migration: %s (%s)", migration.uniqueId.toString, migration.shortName)
    _ <- migration.execute(sinkPropertyManager, context, mergeClient, streamingSource)
    _ <- recordAppliedMigration(migration)
    _ <- zlog("Successfully applied migration: %s (%s)", migration.uniqueId.toString, migration.shortName)
  yield ()).orDieWith(e =>
    FatalStreamFailExceptionWithCause(
      s"Failed to execute migration: ${migration.uniqueId} (${migration.shortName})",
      e
    )
  )

  override def commitMigration: Task[Unit] = for
    _ <- sinkPropertyManager.setProperty(
      targetTableName,
      MetadataKeys.getFrameworkKey(MetadataKeys.migrationKey),
      CurrentMigration.toString
    )
    _ <- sinkPropertyManager.removeProperty(targetTableName, appliedMigrationsProperty)
  yield ()

object DefaultMigrationService:
  val layer = ZLayer {
    for
      sinkPropertyManager <- ZIO.service[SinkPropertyManager]
      context             <- ZIO.service[PluginStreamContext]
      mergeClient         <- ZIO.service[MergeServiceClient]
      streamingSource     <- ZIO.service[StreamingSource]
    yield new DefaultMigrationService(sinkPropertyManager, context, mergeClient, streamingSource)
  }
