package com.sneaksanddata.arcane.framework
package services.synapse

import logging.ZIOLogAnnotations.*
import models.app.StreamContext
import models.schemas.DataRow
import models.settings.{BackfillSettings, VersionedDataGraphBuilderSettings}
import services.metrics.DeclaredMetrics
import services.streaming.base.{DefaultStreamDataProvider, StreamDataProvider}
import services.synapse.base.SynapseLinkDataProvider
import services.synapse.versioning.SynapseWatermark

import zio.metrics.Metric
import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

class SynapseLinkStreamingDataProvider(
    dataProvider: SynapseLinkDataProvider,
    settings: VersionedDataGraphBuilderSettings,
    backfillSettings: BackfillSettings,
    streamContext: StreamContext,
    metrics: DeclaredMetrics
) extends DefaultStreamDataProvider[SynapseWatermark, DataRow](dataProvider, settings, backfillSettings, streamContext)
  
  // TODO: reimplement 
  //  private val batchDelayInterval = metrics.tagMetric(Metric.gauge("arcane.stream.synapse.processing_lag"))

object SynapseLinkStreamingDataProvider:

  /** The environment for the MsSqlStreamingDataProvider.
    */
  type Environment = SynapseLinkDataProvider & VersionedDataGraphBuilderSettings & BackfillSettings & StreamContext & DeclaredMetrics

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
      metrics: DeclaredMetrics
  ): SynapseLinkStreamingDataProvider =
    new SynapseLinkStreamingDataProvider(dataProvider, settings, backfillSettings, streamContext, metrics)

  /** The ZLayer that creates the MsSqlStreamingDataProvider.
    */
  val layer: ZLayer[Environment, Nothing, StreamDataProvider] =
    ZLayer {
      for
        dataProvider  <- ZIO.service[SynapseLinkDataProvider]
        settings      <- ZIO.service[VersionedDataGraphBuilderSettings]
        backfillSettings      <- ZIO.service[BackfillSettings]
        streamContext <- ZIO.service[StreamContext]
        metrics       <- ZIO.service[DeclaredMetrics]
      yield SynapseLinkStreamingDataProvider(dataProvider, settings, backfillSettings, streamContext, metrics)
    }
