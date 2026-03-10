package com.sneaksanddata.arcane.framework
package services.synapse

import models.app.{BaseStreamContext, PluginStreamContext}
import models.schemas.DataRow
import models.settings.backfill.BackfillSettings
import models.settings.streaming.ChangeCaptureSettings
import services.metrics.DeclaredMetrics
import services.streaming.base.{DefaultStreamDataProvider, StreamDataProvider}
import services.synapse.base.SynapseLinkDataProvider
import services.synapse.versioning.SynapseWatermark

import zio.{ZIO, ZLayer}

class SynapseLinkStreamingDataProvider(
    dataProvider: SynapseLinkDataProvider,
    settings: ChangeCaptureSettings,
    backfillSettings: BackfillSettings,
    isBackfilling: Boolean,
    declaredMetrics: DeclaredMetrics
) extends DefaultStreamDataProvider[SynapseWatermark, DataRow](
      dataProvider,
      settings,
      backfillSettings,
      isBackfilling,
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
      backfillSettings: BackfillSettings,
      isBackfilling: Boolean,
      declaredMetrics: DeclaredMetrics
  ): SynapseLinkStreamingDataProvider =
    new SynapseLinkStreamingDataProvider(dataProvider, settings, backfillSettings, isBackfilling, declaredMetrics)

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
        context.streamMode.backfill,
        context.isBackfilling,
        declaredMetrics
      )
    }
