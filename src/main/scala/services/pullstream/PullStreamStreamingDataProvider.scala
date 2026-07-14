package com.sneaksanddata.arcane.framework
package services.pullstream

import models.app.PluginStreamContext
import models.settings.streaming.ChangeCaptureSettings
import services.metrics.DeclaredMetrics
import services.pullstream.versioning.PullStreamWatermark
import services.streaming.base.{DefaultStreamDataProvider, StreamDataProvider}

import zio.{ZIO, ZLayer}

class PullStreamStreamingDataProvider(
    dataProvider: PullStreamSourceDataProvider,
    settings: ChangeCaptureSettings,
    declaredMetrics: DeclaredMetrics
) extends DefaultStreamDataProvider[PullStreamWatermark](
      dataProvider,
      settings,
      declaredMetrics
    )

object PullStreamStreamingDataProvider:
  private type Environment =
    PullStreamSourceDataProvider & PluginStreamContext & DeclaredMetrics

  def apply(
      dataProvider: PullStreamSourceDataProvider,
      settings: ChangeCaptureSettings,
      declaredMetrics: DeclaredMetrics
  ): PullStreamStreamingDataProvider =
    new PullStreamStreamingDataProvider(dataProvider, settings, declaredMetrics)

  val layer: ZLayer[Environment, Nothing, StreamDataProvider] = ZLayer {
    for
      context         <- ZIO.service[PluginStreamContext]
      dataProvider    <- ZIO.service[PullStreamSourceDataProvider]
      declaredMetrics <- ZIO.service[DeclaredMetrics]
    yield PullStreamStreamingDataProvider(
      dataProvider,
      context.streamMode.changeCapture,
      declaredMetrics
    )
  }
