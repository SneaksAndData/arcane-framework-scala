package com.sneaksanddata.arcane.framework
package services.blobsource.providers

import logging.ZIOLogAnnotations.zlog
import models.app.StreamContext
import models.schemas.DataRow
import models.settings.VersionedDataGraphBuilderSettings
import services.blobsource.BlobSourceBatch
import services.blobsource.versioning.BlobSourceWatermark
import services.metrics.DeclaredMetrics
import services.streaming.base.{DefaultStreamDataProvider, StreamDataProvider}
import com.sneaksanddata.arcane.framework.models.settings.backfill.BackfillSettings

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

class BlobSourceStreamingDataProvider(
    dataProvider: BlobSourceDataProvider,
    settings: VersionedDataGraphBuilderSettings,
    backfillSettings: BackfillSettings,
    streamContext: StreamContext,
    declaredMetrics: DeclaredMetrics
) extends DefaultStreamDataProvider[BlobSourceWatermark, BlobSourceBatch](
      dataProvider,
      settings,
      backfillSettings,
      streamContext,
      declaredMetrics
    )

object BlobSourceStreamingDataProvider:
  private type Environment = BlobSourceDataProvider & VersionedDataGraphBuilderSettings & BackfillSettings &
    StreamContext & DeclaredMetrics

  def apply(
      dataProvider: BlobSourceDataProvider,
      settings: VersionedDataGraphBuilderSettings,
      backfillSettings: BackfillSettings,
      streamContext: StreamContext,
      declaredMetrics: DeclaredMetrics
  ): BlobSourceStreamingDataProvider =
    new BlobSourceStreamingDataProvider(dataProvider, settings, backfillSettings, streamContext, declaredMetrics)

  val layer: ZLayer[Environment, Nothing, StreamDataProvider] =
    ZLayer {
      for
        dataProvider     <- ZIO.service[BlobSourceDataProvider]
        settings         <- ZIO.service[VersionedDataGraphBuilderSettings]
        backfillSettings <- ZIO.service[BackfillSettings]
        streamContext    <- ZIO.service[StreamContext]
        declaredMetrics  <- ZIO.service[DeclaredMetrics]
      yield BlobSourceStreamingDataProvider(dataProvider, settings, backfillSettings, streamContext, declaredMetrics)
    }
