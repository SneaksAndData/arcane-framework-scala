package com.sneaksanddata.arcane.framework
package services.blobsource.providers

import models.app.PluginStreamContext
import models.settings.streaming.ChangeCaptureSettings
import services.blobsource.versioning.BlobSourceWatermark
import services.metrics.DeclaredMetrics
import services.streaming.base.{DefaultStreamDataProvider, StreamDataProvider}

import zio.{ZIO, ZLayer}

class BlobSourceStreamingDataProvider(
    dataProvider: BlobSourceDataProvider,
    settings: ChangeCaptureSettings,
    declaredMetrics: DeclaredMetrics
) extends DefaultStreamDataProvider[BlobSourceWatermark](
      dataProvider,
      settings,
      declaredMetrics
    )

object BlobSourceStreamingDataProvider:
  private type Environment = BlobSourceDataProvider & PluginStreamContext & DeclaredMetrics

  def apply(
      dataProvider: BlobSourceDataProvider,
      settings: ChangeCaptureSettings,
      declaredMetrics: DeclaredMetrics
  ): BlobSourceStreamingDataProvider =
    new BlobSourceStreamingDataProvider(dataProvider, settings, declaredMetrics)

  val layer: ZLayer[Environment, Nothing, StreamDataProvider] =
    ZLayer {
      for
        context         <- ZIO.service[PluginStreamContext]
        dataProvider    <- ZIO.service[BlobSourceDataProvider]
        declaredMetrics <- ZIO.service[DeclaredMetrics]
      yield BlobSourceStreamingDataProvider(
        dataProvider,
        context.streamMode.changeCapture,
        declaredMetrics
      )
    }
