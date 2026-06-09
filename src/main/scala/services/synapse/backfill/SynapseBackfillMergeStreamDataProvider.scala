package com.sneaksanddata.arcane.framework
package services.synapse.backfill

import models.app.PluginStreamContext
import models.settings.backfill.BackfillSettings
import services.backfill.DefaultBackfillMergeStreamDataProvider
import services.backfill.base.BackfillSourceDataProvider
import services.streaming.base.ChangeCaptureDataProvider
import services.synapse.versioning.SynapseWatermark

import zio.{ZIO, ZLayer}

final class SynapseBackfillMergeStreamDataProvider(
    dataProvider: ChangeCaptureDataProvider[SynapseWatermark],
    backfillSourceDataProvider: BackfillSourceDataProvider[SynapseWatermark],
    backfillSettings: BackfillSettings
) extends DefaultBackfillMergeStreamDataProvider[SynapseWatermark](
      dataProvider,
      backfillSourceDataProvider,
      backfillSettings
    )

object SynapseBackfillMergeStreamDataProvider:
  val layer = ZLayer {
    for
      dataProvider               <- ZIO.service[ChangeCaptureDataProvider[SynapseWatermark]]
      backfillSourceDataProvider <- ZIO.service[BackfillSourceDataProvider[SynapseWatermark]]
      context                    <- ZIO.service[PluginStreamContext]
    yield new SynapseBackfillMergeStreamDataProvider(
      dataProvider = dataProvider,
      backfillSourceDataProvider = backfillSourceDataProvider,
      backfillSettings = context.streamMode.backfill
    )
  }
