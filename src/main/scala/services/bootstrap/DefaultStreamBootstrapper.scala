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
import services.base.SchemaProvider
import services.bootstrap.base.StreamBootstrapper
import services.iceberg.base.{SinkEntityManager, StagingEntityManager}
import services.iceberg.given_Conversion_ArcaneSchema_Schema

import zio.{Task, ZIO, ZLayer}

class DefaultStreamBootstrapper(
    stagingEntityManager: StagingEntityManager,
    sinkEntityManager: SinkEntityManager,
    schemaProvider: SchemaProvider[ArcaneSchema],
    sinkSettings: SinkSettings,
    stagingSettings: StagingSettings,
    backfillSettings: BackfillSettings,
    isBackfilling: Boolean
) extends StreamBootstrapper:
  override def cleanupStagingTables(prefix: String): Task[Unit] =
    zlog("Looking for staging tables from previous run, using prefix %s", prefix) *> stagingEntityManager.deleteTables(
      prefix
    )

  override def createBackFillTable: Task[Unit] =
    for _ <- ZIO.when(isBackfilling && backfillSettings.backfillBehavior == Overwrite) {
        for
          schema <- schemaProvider.getSchema
          _      <- zlog("Creating backfill table %s", stagingSettings.table.backfillTableName)
          _ <- stagingEntityManager.createTable(
            CreateTableRequest(
              name = stagingSettings.table.backfillTableName,
              schema = schema,
              replace = false
            )
          )
        yield ()
      }
    yield ()

  override def createTargetTable: Task[Unit] = for
    schema <- schemaProvider.getSchema
    _      <- zlog("Creating target table %s", sinkSettings.targetTableFullName)
    _ <- sinkEntityManager.createTable(
      CreateTableRequest(
        name = sinkSettings.targetTableFullName.parts.name,
        schema = schema,
        replace = false
        // TODO: https://github.com/SneaksAndData/arcane-framework-scala/issues/307
      )
    )
  yield ()

object DefaultStreamBootstrapper:
  def apply(
      stagingEntityManager: StagingEntityManager,
      sinkEntityManager: SinkEntityManager,
      schemaProvider: SchemaProvider[ArcaneSchema],
      sinkSettings: SinkSettings,
      stagingSettings: StagingSettings,
      backfillSettings: BackfillSettings,
      isBackfilling: Boolean
  ): DefaultStreamBootstrapper = new DefaultStreamBootstrapper(
    stagingEntityManager = stagingEntityManager,
    sinkEntityManager = sinkEntityManager,
    schemaProvider = schemaProvider,
    sinkSettings = sinkSettings,
    stagingSettings = stagingSettings,
    backfillSettings = backfillSettings,
    isBackfilling = isBackfilling
  )

  val layer = ZLayer {
    for
      context              <- ZIO.service[PluginStreamContext]
      stagingEntityManager <- ZIO.service[StagingEntityManager]
      sinkEntityManager    <- ZIO.service[SinkEntityManager]
      schemaProvider       <- ZIO.service[SchemaProvider[ArcaneSchema]]
    yield DefaultStreamBootstrapper(
      stagingEntityManager = stagingEntityManager,
      sinkEntityManager = sinkEntityManager,
      schemaProvider = schemaProvider,
      sinkSettings = context.sink,
      stagingSettings = context.staging,
      backfillSettings = context.streamMode.backfill,
      isBackfilling = context.isBackfilling
    )
  }
