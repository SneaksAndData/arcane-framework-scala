package com.sneaksanddata.arcane.framework
package services.blobsource.backfill

import models.app.PluginStreamContext
import models.settings.backfill.BackfillSettings
import services.backfill.{DefaultBackfillStateManager, DefaultShardedBackfillStreamDataProvider}
import services.blobsource.versioning.BlobSourceWatermark
import services.metrics.DeclaredMetrics

import zio.{ZIO, ZLayer}

class BlobShardedBackfillStreamDataProvider(
    dataProvider: BlobBackfillSourceDataProvider,
    backfillSettings: BackfillSettings,
    stateManager: DefaultBackfillStateManager,
    declaredMetrics: DeclaredMetrics
) extends DefaultShardedBackfillStreamDataProvider[BlobSourceWatermark](
      dataProvider,
      backfillSettings,
      stateManager,
      declaredMetrics
    )

object BlobShardedBackfillStreamDataProvider:
  val layer = ZLayer {
    for
      dataProvider <- ZIO.service[BlobBackfillSourceDataProvider]
      context      <- ZIO.service[PluginStreamContext]
      stateManager <- ZIO.service[DefaultBackfillStateManager]
      metrics      <- ZIO.service[DeclaredMetrics]
    yield new BlobShardedBackfillStreamDataProvider(
      dataProvider = dataProvider,
      backfillSettings = context.streamMode.backfill,
      stateManager = stateManager,
      declaredMetrics = metrics
    )
  }
