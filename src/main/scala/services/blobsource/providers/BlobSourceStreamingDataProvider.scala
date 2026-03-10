package com.sneaksanddata.arcane.framework
package services.blobsource.providers

import models.app.{BaseStreamContext, PluginStreamContext}
import models.settings.backfill.BackfillSettings
import models.settings.streaming.ChangeCaptureSettings
import services.blobsource.BlobSourceBatch
import services.blobsource.versioning.BlobSourceWatermark
import services.metrics.DeclaredMetrics
import services.streaming.base.{DefaultStreamDataProvider, StreamDataProvider}

import zio.{ZIO, ZLayer}

class BlobSourceStreamingDataProvider(
    dataProvider: BlobSourceDataProvider,
    settings: ChangeCaptureSettings,
    backfillSettings: BackfillSettings,
    isBackfilling: Boolean,
    declaredMetrics: DeclaredMetrics
) extends DefaultStreamDataProvider[BlobSourceWatermark, BlobSourceBatch](
      dataProvider,
      settings,
      backfillSettings,
  isBackfilling,
      declaredMetrics
    )

object BlobSourceStreamingDataProvider:
  private type Environment = BlobSourceDataProvider & PluginStreamContext & DeclaredMetrics

  def apply(
      dataProvider: BlobSourceDataProvider,
      settings: ChangeCaptureSettings,
      backfillSettings: BackfillSettings,
      isBackfilling: Boolean,
      declaredMetrics: DeclaredMetrics
  ): BlobSourceStreamingDataProvider =
    new BlobSourceStreamingDataProvider(dataProvider, settings, backfillSettings, isBackfilling, declaredMetrics)

  val layer: ZLayer[Environment, Nothing, StreamDataProvider] =
    ZLayer {
      for
        context <- ZIO.service[PluginStreamContext]
        dataProvider     <- ZIO.service[BlobSourceDataProvider]
        declaredMetrics  <- ZIO.service[DeclaredMetrics]
      yield BlobSourceStreamingDataProvider(dataProvider, context.streamMode.changeCapture, context.streamMode.backfill, context.IsBackfilling, declaredMetrics)
    }
