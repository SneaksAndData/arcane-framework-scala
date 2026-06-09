package com.sneaksanddata.arcane.framework
package services.blobsource.backfill

import models.app.PluginStreamContext
import models.settings.backfill.BackfillSettings
import services.backfill.DefaultBackfillMergeStreamDataProvider
import services.backfill.base.BackfillSourceDataProvider
import services.blobsource.versioning.BlobSourceWatermark
import services.streaming.base.ChangeCaptureDataProvider

import zio.{ZIO, ZLayer}

final class BlobSourceBackfillMergeStreamDataProvider(
    dataProvider: ChangeCaptureDataProvider[BlobSourceWatermark],
    backfillSourceDataProvider: BackfillSourceDataProvider[BlobSourceWatermark],
    backfillSettings: BackfillSettings
) extends DefaultBackfillMergeStreamDataProvider[BlobSourceWatermark](
      dataProvider,
      backfillSourceDataProvider,
      backfillSettings
    )

object BlobSourceBackfillMergeStreamDataProvider:
  val layer = ZLayer {
    for
      dataProvider               <- ZIO.service[ChangeCaptureDataProvider[BlobSourceWatermark]]
      backfillSourceDataProvider <- ZIO.service[BackfillSourceDataProvider[BlobSourceWatermark]]
      context                    <- ZIO.service[PluginStreamContext]
    yield new BlobSourceBackfillMergeStreamDataProvider(
      dataProvider = dataProvider,
      backfillSourceDataProvider = backfillSourceDataProvider,
      backfillSettings = context.streamMode.backfill
    )
  }
