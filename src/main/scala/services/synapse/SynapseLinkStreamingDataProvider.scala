package com.sneaksanddata.arcane.framework
package services.synapse

import models.app.PluginStreamContext
import models.settings.streaming.ChangeCaptureSettings
import services.metrics.DeclaredMetrics
import services.streaming.base.{DefaultStreamDataProvider, StreamDataProvider}
import services.synapse.base.SynapseLinkDataProvider
import services.synapse.versioning.SynapseWatermark

import zio.{ZIO, ZLayer}

class SynapseLinkStreamingDataProvider(
    dataProvider: SynapseLinkDataProvider,
    settings: ChangeCaptureSettings,
    declaredMetrics: DeclaredMetrics
) extends DefaultStreamDataProvider[SynapseWatermark](
      dataProvider,
      settings,
      declaredMetrics
    )

object SynapseLinkStreamingDataProvider:

  /** The environment for the MsSqlStreamingDataProvider.
    */
  type Environment = SynapseLinkDataProvider & PluginStreamContext & DeclaredMetrics

  /** Creates a new instance of the MsSqlStreamingDataProvider class.
    * @param dataProvider
    *   Underlying data provider.
    * @return
    *   A new instance of the MsSqlStreamingDataProvider class.
    */
  def apply(
      dataProvider: SynapseLinkDataProvider,
      settings: ChangeCaptureSettings,
      declaredMetrics: DeclaredMetrics
  ): SynapseLinkStreamingDataProvider =
    new SynapseLinkStreamingDataProvider(dataProvider, settings, declaredMetrics)

  /** The ZLayer that creates the MsSqlStreamingDataProvider.
    */
  val layer: ZLayer[Environment, Nothing, StreamDataProvider] =
    ZLayer {
      for
        context         <- ZIO.service[PluginStreamContext]
        dataProvider    <- ZIO.service[SynapseLinkDataProvider]
        declaredMetrics <- ZIO.service[DeclaredMetrics]
      yield SynapseLinkStreamingDataProvider(
        dataProvider,
        context.streamMode.changeCapture,
        declaredMetrics
      )
    }
