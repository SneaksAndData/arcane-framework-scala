package com.sneaksanddata.arcane.framework
package services.bootstrap

import logging.ZIOLogAnnotations.zlog
import models.app.PluginStreamContext
import models.ddl.CreateTableRequest
import models.schemas.ArcaneSchema
import models.settings.TableNaming.*
import models.settings.backfill.BackfillBehavior.Overwrite
import models.settings.backfill.BackfillSettings
import models.settings.sink.SinkSettings
import models.settings.staging.StagingSettings
import services.base.{SchemaProvider, StreamingSource}
import services.bootstrap.base.StreamBootstrapper
import services.iceberg.base.{SinkEntityManager, SinkPropertyManager, StagingEntityManager}
import services.iceberg.{given_Conversion_ArcaneSchema_Schema, given_Conversion_Schema_ArcaneSchema}

import zio.{Task, ZIO, ZLayer}

class DefaultStreamBootstrapper(
                                 stagingEntityManager: StagingEntityManager,
                                 sinkEntityManager: SinkEntityManager,
                                 sinkPropertyManager: SinkPropertyManager,
                                 streamingSource: StreamingSource,
                                 sinkSettings: SinkSettings,
                                 stagingSettings: StagingSettings,
                                 backfillSettings: BackfillSettings,
                                 isBackfilling: Boolean,
                                 backfillId: Option[String]
) extends StreamBootstrapper:
  override def cleanupStagingTables(prefix: String): Task[Unit] =
    zlog("Looking for staging tables from previous run, using prefix %s", prefix) *> stagingEntityManager.deleteTables(
      prefix
    )

  override def cleanupOutdatedBackfill: Task[Unit] = ZIO
    .unless(isBackfilling && backfillId.isDefined)(
      zlog("Looking for outdated backfill tables") *> stagingEntityManager.deleteTables(
        // TODO: include stream id in backfill table names so they can be identified
        "backfill_"
      )
    )
    .map(_ => ())

  override def createBackFillTable: Task[Unit] =
    for _ <- ZIO.when(isBackfilling && backfillId.isDefined) {
        for
          schema <- streamingSource.getSchema
          _      <- zlog("Creating backfill table %s", getBackfillTableName(backfillId.get))
          _ <- stagingEntityManager.createTable(
            CreateTableRequest(
              name = getBackfillTableName(backfillId.get),
              schema = schema,
              replace = false
            )
          )
        yield ()
      }
    yield ()

  override def createTargetTable: Task[Unit] = for
    schema <- streamingSource.getSchema
    _      <- zlog("Creating target table %s", sinkSettings.targetTableFullName)
    _ <- sinkEntityManager.createTable(
      CreateTableRequest(
        name = sinkSettings.targetTableFullName.parts.name,
        schema = schema,
        replace = false
        // TODO: https://github.com/SneaksAndData/arcane-framework-scala/issues/307
      )
    )
    // pre-migrate sources with unified schema and skip migration of each staged batch
    _ <- ZIO.when(stagingSettings.table.isUnifiedSchema) {
      for
        targetSchema <- sinkPropertyManager.getTableSchema(sinkSettings.targetTableFullName.parts.name)
        // note that if a table was not created by Arcane, this will lead to merge failure later on, as we assume target schema has MergeKeyField
        // in case when a target was not created by Arcane, it should be dropped and re-created by the bootstrapper
        _ <- sinkEntityManager.migrateSchema(targetSchema, schema, sinkSettings.targetTableFullName.parts.name)
      yield ()
    }
  yield ()

object DefaultStreamBootstrapper:
  def apply(
      stagingEntityManager: StagingEntityManager,
      sinkEntityManager: SinkEntityManager,
      sinkPropertyManager: SinkPropertyManager,
      streamingSource: StreamingSource,
      sinkSettings: SinkSettings,
      stagingSettings: StagingSettings,
      backfillSettings: BackfillSettings,
      isBackfilling: Boolean,
      backfillId: Option[String]
  ): DefaultStreamBootstrapper = new DefaultStreamBootstrapper(
    stagingEntityManager = stagingEntityManager,
    sinkEntityManager = sinkEntityManager,
    sinkPropertyManager = sinkPropertyManager,
    streamingSource = streamingSource,
    sinkSettings = sinkSettings,
    stagingSettings = stagingSettings,
    backfillSettings = backfillSettings,
    isBackfilling = isBackfilling,
    backfillId = backfillId
  )

  val layer = ZLayer {
    for
      context              <- ZIO.service[PluginStreamContext]
      stagingEntityManager <- ZIO.service[StagingEntityManager]
      sinkEntityManager    <- ZIO.service[SinkEntityManager]
      sinkPropertyManager  <- ZIO.service[SinkPropertyManager]
      streamingSource       <- ZIO.service[StreamingSource]
      isBackfilling        <- context.isBackfilling.orElseSucceed(false)
      backfillId           <- ZIO.when(isBackfilling)(context.backfillId)
    yield DefaultStreamBootstrapper(
      stagingEntityManager = stagingEntityManager,
      sinkEntityManager = sinkEntityManager,
      sinkPropertyManager = sinkPropertyManager,
      streamingSource = streamingSource,
      sinkSettings = context.sink,
      stagingSettings = context.staging,
      backfillSettings = context.streamMode.backfill,
      isBackfilling = isBackfilling,
      backfillId = backfillId
    )
  }
