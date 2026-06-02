package com.sneaksanddata.arcane.framework
package services.bootstrap

import logging.ZIOLogAnnotations.zlog
import models.app.PluginStreamContext
import models.ddl.CreateTableRequest
import models.settings.staging.StagingSettings
import services.base.StreamingSource
import services.bootstrap.base.StreamBootstrapper
import services.iceberg.base.{SinkEntityManager, SinkPropertyManager, StagingEntityManager}
import services.iceberg.{given_Conversion_ArcaneSchema_Schema, given_Conversion_Schema_ArcaneSchema}
import services.naming.NameGenerator

import zio.{Task, ZIO, ZLayer}

class DefaultStreamBootstrapper(
    stagingEntityManager: StagingEntityManager,
    sinkEntityManager: SinkEntityManager,
    sinkPropertyManager: SinkPropertyManager,
    streamingSource: StreamingSource,
    nameGenerator: NameGenerator,
    stagingSettings: StagingSettings,
    isBackfilling: Boolean
) extends StreamBootstrapper:
  override def cleanupStagingTables: Task[Unit] = for
    prefix <- nameGenerator.getStagingTablePrefix
    _      <- zlog("Looking for staging tables from previous run, using prefix %s", prefix)
    _      <- stagingEntityManager.deleteTables(prefix)
  yield ()

  override def cleanupOutdatedBackfill: Task[Unit] = for _ <- ZIO.unless(isBackfilling) {
      for
        _      <- zlog("Looking for outdated backfill tables")
        prefix <- nameGenerator.getBackfillTablesPrefix.map(v => s"${v}__")
        _      <- stagingEntityManager.deleteTables(prefix)
        _      <- streamingSource.deleteShards(prefix)
      yield ()
    }
  yield ()

  override def createBackFillTable: Task[Unit] =
    for _ <- ZIO.when(isBackfilling) {
        for
          schema    <- streamingSource.getSchema
          tableName <- nameGenerator.getBackfillTableName
          _         <- zlog("Creating backfill table %s", tableName)
          _ <- stagingEntityManager.createTable(
            CreateTableRequest(
              name = tableName,
              schema = schema,
              replace = false
            )
          )
        yield ()
      }
    yield ()

  override def createTargetTable: Task[Unit] = for
    schema         <- streamingSource.getSchema
    targetFullName <- nameGenerator.getTargetTableFullName
    targetName     <- nameGenerator.getTargetTableName
    _              <- zlog("Creating target table %s", targetFullName)
    _ <- sinkEntityManager.createTable(
      CreateTableRequest(
        name = targetName,
        schema = schema,
        replace = false
        // TODO: https://github.com/SneaksAndData/arcane-framework-scala/issues/307
      )
    )
    // pre-migrate sources with unified schema and skip migration of each staged batch
    _ <- ZIO.when(stagingSettings.table.isUnifiedSchema) {
      for
        targetSchema <- sinkPropertyManager.getTableSchema(targetName)
        // note that if a table was not created by Arcane, this will lead to merge failure later on, as we assume target schema has MergeKeyField
        // in case when a target was not created by Arcane, it should be dropped and re-created by the bootstrapper
        _ <- sinkEntityManager.migrateSchema(targetSchema, schema, targetName)
      yield ()
    }
  yield ()

object DefaultStreamBootstrapper:
  def apply(
      stagingEntityManager: StagingEntityManager,
      sinkEntityManager: SinkEntityManager,
      sinkPropertyManager: SinkPropertyManager,
      streamingSource: StreamingSource,
      nameGenerator: NameGenerator,
      stagingSettings: StagingSettings,
      isBackfilling: Boolean
  ): DefaultStreamBootstrapper = new DefaultStreamBootstrapper(
    stagingEntityManager = stagingEntityManager,
    sinkEntityManager = sinkEntityManager,
    sinkPropertyManager = sinkPropertyManager,
    streamingSource = streamingSource,
    nameGenerator = nameGenerator,
    stagingSettings = stagingSettings,
    isBackfilling = isBackfilling
  )

  val layer = ZLayer {
    for
      context              <- ZIO.service[PluginStreamContext]
      stagingEntityManager <- ZIO.service[StagingEntityManager]
      sinkEntityManager    <- ZIO.service[SinkEntityManager]
      sinkPropertyManager  <- ZIO.service[SinkPropertyManager]
      streamingSource      <- ZIO.service[StreamingSource]
      isBackfilling        <- context.isBackfilling.orElseSucceed(false)
      nameGenerator        <- ZIO.service[NameGenerator]
    yield DefaultStreamBootstrapper(
      stagingEntityManager = stagingEntityManager,
      sinkEntityManager = sinkEntityManager,
      sinkPropertyManager = sinkPropertyManager,
      streamingSource = streamingSource,
      nameGenerator = nameGenerator,
      stagingSettings = context.staging,
      isBackfilling = isBackfilling
    )
  }
