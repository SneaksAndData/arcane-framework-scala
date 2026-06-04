package com.sneaksanddata.arcane.framework
package services.synapse.backfill

import models.app.PluginStreamContext
import models.settings.backfill.BackfillSettings
import services.backfill.{DefaultBackfillStateManager, DefaultShardedBackfillStreamDataProvider}
import services.metrics.DeclaredMetrics
import services.synapse.versioning.SynapseWatermark

import zio.{ZIO, ZLayer}

class SynapseShardedBackfillStreamDataProvider(
    dataProvider: SynapseBackfillSourceDataProvider,
    backfillSettings: BackfillSettings,
    stateManager: DefaultBackfillStateManager,
    declaredMetrics: DeclaredMetrics
) extends DefaultShardedBackfillStreamDataProvider[SynapseWatermark](
      dataProvider,
      backfillSettings,
      stateManager,
      declaredMetrics
    )

object SynapseShardedBackfillStreamDataProvider:
  val layer = ZLayer {
    for
      dataProvider <- ZIO.service[SynapseBackfillSourceDataProvider]
      context      <- ZIO.service[PluginStreamContext]
      stateManager <- ZIO.service[DefaultBackfillStateManager]
      metrics      <- ZIO.service[DeclaredMetrics]
    yield new SynapseShardedBackfillStreamDataProvider(
      dataProvider = dataProvider,
      backfillSettings = context.streamMode.backfill,
      stateManager = stateManager,
      declaredMetrics = metrics
    )
  }
