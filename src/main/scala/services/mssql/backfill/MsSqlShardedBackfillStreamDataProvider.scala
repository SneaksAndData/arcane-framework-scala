package com.sneaksanddata.arcane.framework
package services.mssql.backfill

import models.app.PluginStreamContext
import models.settings.backfill.BackfillSettings
import services.backfill.{DefaultBackfillStateManager, DefaultShardedBackfillStreamDataProvider}
import services.metrics.DeclaredMetrics
import services.mssql.versioning.MsSqlWatermark

import zio.{ZIO, ZLayer}

class MsSqlShardedBackfillStreamDataProvider(
    dataProvider: MsSqlBackfillSourceDataProvider,
    backfillSettings: BackfillSettings,
    stateManager: DefaultBackfillStateManager,
    declaredMetrics: DeclaredMetrics
) extends DefaultShardedBackfillStreamDataProvider[MsSqlWatermark](
      dataProvider,
      backfillSettings,
      stateManager,
      declaredMetrics
    )

object MsSqlShardedBackfillStreamDataProvider:
  val layer = ZLayer {
    for
      dataProvider <- ZIO.service[MsSqlBackfillSourceDataProvider]
      context      <- ZIO.service[PluginStreamContext]
      stateManager <- ZIO.service[DefaultBackfillStateManager]
      metrics      <- ZIO.service[DeclaredMetrics]
    yield new MsSqlShardedBackfillStreamDataProvider(
      dataProvider = dataProvider,
      backfillSettings = context.streamMode.backfill,
      stateManager = stateManager,
      declaredMetrics = metrics
    )
  }
