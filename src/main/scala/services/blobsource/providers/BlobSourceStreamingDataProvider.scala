package com.sneaksanddata.arcane.framework
package services.blobsource.providers

import models.app.BaseStreamContext
import models.settings.backfill.BackfillSettings
import services.blobsource.BlobSourceBatch
import services.blobsource.versioning.BlobSourceWatermark
import services.metrics.DeclaredMetrics
import services.streaming.base.{DefaultStreamDataProvider, StreamDataProvider}
import com.sneaksanddata.arcane.framework.models.settings.streaming.ChangeCaptureSettings

import zio.{ZIO, ZLayer}

class BlobSourceStreamingDataProvider(
    dataProvider: BlobSourceDataProvider,
    settings: ChangeCaptureSettings,
    backfillSettings: BackfillSettings,
    streamContext: BaseStreamContext,
    declaredMetrics: DeclaredMetrics
) extends DefaultStreamDataProvider[BlobSourceWatermark, BlobSourceBatch](
      dataProvider,
      settings,
      backfillSettings,
      streamContext,
      declaredMetrics
    )

object BlobSourceStreamingDataProvider:
  private type Environment = BlobSourceDataProvider & ChangeCaptureSettings & BackfillSettings & BaseStreamContext &
    DeclaredMetrics

  def apply(
      dataProvider: BlobSourceDataProvider,
      settings: ChangeCaptureSettings,
      backfillSettings: BackfillSettings,
      streamContext: BaseStreamContext,
      declaredMetrics: DeclaredMetrics
  ): BlobSourceStreamingDataProvider =
    new BlobSourceStreamingDataProvider(dataProvider, settings, backfillSettings, streamContext, declaredMetrics)

  val layer: ZLayer[Environment, Nothing, StreamDataProvider] =
    ZLayer {
      for
        dataProvider     <- ZIO.service[BlobSourceDataProvider]
        settings         <- ZIO.service[ChangeCaptureSettings]
        backfillSettings <- ZIO.service[BackfillSettings]
        streamContext    <- ZIO.service[BaseStreamContext]
        declaredMetrics  <- ZIO.service[DeclaredMetrics]
      yield BlobSourceStreamingDataProvider(dataProvider, settings, backfillSettings, streamContext, declaredMetrics)
    }
