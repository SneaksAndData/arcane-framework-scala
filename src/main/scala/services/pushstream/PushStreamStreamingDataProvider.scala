package com.sneaksanddata.arcane.framework
package services.pushstream

import models.app.PluginStreamContext
import models.settings.streaming.ChangeCaptureSettings
import services.metrics.DeclaredMetrics
import services.pushstream.versioning.PushStreamWatermark
import services.streaming.base.{DefaultStreamDataProvider, StreamDataProvider}

import zio.{ZIO, ZLayer}

class PushStreamStreamingDataProvider(
    dataProvider: PushStreamSourceDataProvider,
    settings: ChangeCaptureSettings,
    declaredMetrics: DeclaredMetrics
) extends DefaultStreamDataProvider[PushStreamWatermark](
      dataProvider,
      settings,
      declaredMetrics
    )

object PushStreamStreamingDataProvider:
  private type Environment =
    PushStreamSourceDataProvider & PluginStreamContext & DeclaredMetrics

  def apply(
      dataProvider: PushStreamSourceDataProvider,
      settings: ChangeCaptureSettings,
      declaredMetrics: DeclaredMetrics
  ): PushStreamStreamingDataProvider =
    new PushStreamStreamingDataProvider(dataProvider, settings, declaredMetrics)

  val layer: ZLayer[Environment, Nothing, StreamDataProvider] = ZLayer {
    for
      context         <- ZIO.service[PluginStreamContext]
      dataProvider    <- ZIO.service[PushStreamSourceDataProvider]
      declaredMetrics <- ZIO.service[DeclaredMetrics]
    yield PushStreamStreamingDataProvider(
      dataProvider,
      context.streamMode.changeCapture,
      declaredMetrics
    )
  }

