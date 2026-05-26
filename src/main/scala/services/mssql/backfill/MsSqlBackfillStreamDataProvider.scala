package com.sneaksanddata.arcane.framework
package services.mssql.backfill

import models.app.PluginStreamContext
import models.settings.backfill.BackfillSettings
import services.backfill.{DefaultBackfillStateManager, DefaultBackfillStreamDataProvider}
import services.metrics.DeclaredMetrics
import services.mssql.versioning.MsSqlWatermark

import zio.{ZIO, ZLayer}

class MsSqlBackfillStreamDataProvider(
    dataProvider: MsSqlBackfillSourceDataProvider,
    backfillSettings: BackfillSettings,
    stateManager: DefaultBackfillStateManager,
    declaredMetrics: DeclaredMetrics
) extends DefaultBackfillStreamDataProvider[MsSqlWatermark](
      dataProvider,
      backfillSettings,
      stateManager,
      declaredMetrics
    )

object MsSqlBackfillStreamDataProvider:
  val layer = ZLayer {
    for
      dataProvider <- ZIO.service[MsSqlBackfillSourceDataProvider]
      context      <- ZIO.service[PluginStreamContext]
      stateManager <- ZIO.service[DefaultBackfillStateManager]
      metrics      <- ZIO.service[DeclaredMetrics]
    yield new MsSqlBackfillStreamDataProvider(
      dataProvider = dataProvider,
      backfillSettings = context.streamMode.backfill,
      stateManager = stateManager,
      declaredMetrics = metrics
    )
  }
