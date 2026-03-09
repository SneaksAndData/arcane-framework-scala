package com.sneaksanddata.arcane.framework
package services.bootstrap

import logging.ZIOLogAnnotations.zlog
import models.app.BaseStreamContext
import models.ddl.CreateTableRequest
import models.schemas.ArcaneSchema
import models.settings.backfill.BackfillBehavior.Overwrite
import models.settings.backfill.BackfillSettings
import models.settings.sink.SinkSettings
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
    backfillSettings: BackfillSettings,
    streamContext: BaseStreamContext
) extends StreamBootstrapper:
  override def cleanupStagingTables(prefix: String): Task[Unit] =
    zlog("Looking for staging tables from previous run, using prefix %s", prefix) *> stagingEntityManager.deleteTables(
      prefix
    )

  override def createBackFillTable: Task[Unit] =
    for _ <- ZIO.when(streamContext.IsBackfilling && backfillSettings.backfillBehavior == Overwrite) {
        for
          schema <- schemaProvider.getSchema
          _      <- zlog("Creating backfill table %s", backfillSettings.backfillTableFullName)
          _ <- stagingEntityManager.createTable(
            CreateTableRequest(
              name = backfillSettings.backfillTableNameParts.Name,
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
        name = sinkSettings.targetTableNameParts.Name,
        schema = schema,
        replace = false
        // TODO: support partitions and other advanced features later
      )
    )
  yield ()

object DefaultStreamBootstrapper:
  def apply(
      stagingEntityManager: StagingEntityManager,
      sinkEntityManager: SinkEntityManager,
      schemaProvider: SchemaProvider[ArcaneSchema],
      sinkSettings: SinkSettings,
      backfillSettings: BackfillSettings,
      streamContext: BaseStreamContext
  ): DefaultStreamBootstrapper = new DefaultStreamBootstrapper(
    stagingEntityManager = stagingEntityManager,
    sinkEntityManager = sinkEntityManager,
    schemaProvider = schemaProvider,
    sinkSettings = sinkSettings,
    backfillSettings = backfillSettings,
    streamContext = streamContext
  )

  val layer = ZLayer {
    for
      stagingEntityManager <- ZIO.service[StagingEntityManager]
      sinkEntityManager    <- ZIO.service[SinkEntityManager]
      schemaProvider       <- ZIO.service[SchemaProvider[ArcaneSchema]]
      sinkSettings         <- ZIO.service[SinkSettings]
      backfillSettings     <- ZIO.service[BackfillSettings]
      context              <- ZIO.service[BaseStreamContext]
    yield DefaultStreamBootstrapper(
      stagingEntityManager = stagingEntityManager,
      sinkEntityManager = sinkEntityManager,
      schemaProvider = schemaProvider,
      sinkSettings = sinkSettings,
      backfillSettings = backfillSettings,
      streamContext = context
    )
  }
