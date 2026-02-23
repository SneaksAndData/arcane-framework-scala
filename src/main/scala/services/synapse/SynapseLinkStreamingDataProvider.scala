package com.sneaksanddata.arcane.framework
package services.synapse

import models.app.StreamContext
import models.schemas.DataRow
import models.settings.VersionedDataGraphBuilderSettings
import models.settings.backfill.BackfillSettings
import services.metrics.DeclaredMetrics
import services.streaming.base.{DefaultStreamDataProvider, StreamDataProvider}
import services.synapse.base.SynapseLinkDataProvider
import services.synapse.versioning.SynapseWatermark

import zio.{ZIO, ZLayer}

class SynapseLinkStreamingDataProvider(
    dataProvider: SynapseLinkDataProvider,
    settings: VersionedDataGraphBuilderSettings,
    backfillSettings: BackfillSettings,
    streamContext: StreamContext,
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
  type Environment = SynapseLinkDataProvider & VersionedDataGraphBuilderSettings & BackfillSettings & StreamContext &
    DeclaredMetrics

  /** Creates a new instance of the MsSqlStreamingDataProvider class.
    * @param dataProvider
    *   Underlying data provider.
    * @return
    *   A new instance of the MsSqlStreamingDataProvider class.
    */
  def apply(
      dataProvider: SynapseLinkDataProvider,
      settings: VersionedDataGraphBuilderSettings,
      backfillSettings: BackfillSettings,
      streamContext: StreamContext,
      declaredMetrics: DeclaredMetrics
  ): SynapseLinkStreamingDataProvider =
    new SynapseLinkStreamingDataProvider(dataProvider, settings, backfillSettings, streamContext, declaredMetrics)

  /** The ZLayer that creates the MsSqlStreamingDataProvider.
    */
  val layer: ZLayer[Environment, Nothing, StreamDataProvider] =
    ZLayer {
      for
        dataProvider     <- ZIO.service[SynapseLinkDataProvider]
        settings         <- ZIO.service[VersionedDataGraphBuilderSettings]
        backfillSettings <- ZIO.service[BackfillSettings]
        streamContext    <- ZIO.service[StreamContext]
        declaredMetrics  <- ZIO.service[DeclaredMetrics]
      yield SynapseLinkStreamingDataProvider(dataProvider, settings, backfillSettings, streamContext, declaredMetrics)
    }
