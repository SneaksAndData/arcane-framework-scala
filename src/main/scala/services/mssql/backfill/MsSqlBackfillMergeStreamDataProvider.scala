package com.sneaksanddata.arcane.framework
package services.mssql.backfill

import models.app.PluginStreamContext
import models.settings.backfill.BackfillSettings
import services.backfill.DefaultBackfillMergeStreamDataProvider
import services.backfill.base.BackfillSourceDataProvider
import services.mssql.versioning.MsSqlWatermark
import services.streaming.base.ChangeCaptureDataProvider

import zio.{ZIO, ZLayer}

final class MsSqlBackfillMergeStreamDataProvider(
    dataProvider: ChangeCaptureDataProvider[MsSqlWatermark],
    backfillSourceDataProvider: BackfillSourceDataProvider[MsSqlWatermark],
    backfillSettings: BackfillSettings
) extends DefaultBackfillMergeStreamDataProvider[MsSqlWatermark](
      dataProvider,
      backfillSourceDataProvider,
      backfillSettings
    )

object MsSqlBackfillMergeStreamDataProvider:
  val layer = ZLayer {
    for
      dataProvider               <- ZIO.service[ChangeCaptureDataProvider[MsSqlWatermark]]
      backfillSourceDataProvider <- ZIO.service[BackfillSourceDataProvider[MsSqlWatermark]]
      context                    <- ZIO.service[PluginStreamContext]
    yield new MsSqlBackfillMergeStreamDataProvider(
      dataProvider = dataProvider,
      backfillSourceDataProvider = backfillSourceDataProvider,
      backfillSettings = context.streamMode.backfill
    )
  }
