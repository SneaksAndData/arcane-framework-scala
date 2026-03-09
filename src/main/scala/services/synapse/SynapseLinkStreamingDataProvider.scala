package com.sneaksanddata.arcane.framework
package services.synapse

import models.app.BaseStreamContext
import models.schemas.DataRow
import models.settings.backfill.BackfillSettings
import services.metrics.DeclaredMetrics
import services.streaming.base.{DefaultStreamDataProvider, StreamDataProvider}
import services.synapse.base.SynapseLinkDataProvider
import services.synapse.versioning.SynapseWatermark
import com.sneaksanddata.arcane.framework.models.settings.streaming.ChangeCaptureSettings

import zio.{ZIO, ZLayer}

class SynapseLinkStreamingDataProvider(
    dataProvider: SynapseLinkDataProvider,
    settings: ChangeCaptureSettings,
    backfillSettings: BackfillSettings,
    streamContext: BaseStreamContext,
    declaredMetrics: DeclaredMetrics
) extends DefaultStreamDataProvider[SynapseWatermark, DataRow](
      dataProvider,
      settings,
      backfillSettings,
      streamContext,
      declaredMetrics
    )

object SynapseLinkStreamingDataProvider:

  /** The environment for the MsSqlStreamingDataProvider.
    */
  type Environment = SynapseLinkDataProvider & ChangeCaptureSettings & BackfillSettings & BaseStreamContext &
    DeclaredMetrics

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
      streamContext: BaseStreamContext,
      declaredMetrics: DeclaredMetrics
  ): SynapseLinkStreamingDataProvider =
    new SynapseLinkStreamingDataProvider(dataProvider, settings, backfillSettings, streamContext, declaredMetrics)

  /** The ZLayer that creates the MsSqlStreamingDataProvider.
    */
  val layer: ZLayer[Environment, Nothing, StreamDataProvider] =
    ZLayer {
      for
        dataProvider     <- ZIO.service[SynapseLinkDataProvider]
        settings         <- ZIO.service[ChangeCaptureSettings]
        backfillSettings <- ZIO.service[BackfillSettings]
        streamContext    <- ZIO.service[BaseStreamContext]
        declaredMetrics  <- ZIO.service[DeclaredMetrics]
      yield SynapseLinkStreamingDataProvider(dataProvider, settings, backfillSettings, streamContext, declaredMetrics)
    }
