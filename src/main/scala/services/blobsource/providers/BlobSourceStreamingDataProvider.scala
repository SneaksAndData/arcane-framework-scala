package com.sneaksanddata.arcane.framework
package services.blobsource.providers

import logging.ZIOLogAnnotations.zlog
import models.app.StreamContext
import models.schemas.DataRow
import models.settings.{BackfillSettings, VersionedDataGraphBuilderSettings}
import services.blobsource.versioning.BlobSourceWatermark
import services.streaming.base.{DefaultStreamDataProvider, StreamDataProvider}

import com.sneaksanddata.arcane.framework.services.blobsource.BlobSourceBatch
import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

class BlobSourceStreamingDataProvider(
                                       dataProvider: BlobSourceDataProvider,
                                       settings: VersionedDataGraphBuilderSettings,
                                       backfillSettings: BackfillSettings,
                                       streamContext: StreamContext,
) extends DefaultStreamDataProvider[BlobSourceWatermark, BlobSourceBatch](dataProvider, settings, backfillSettings, streamContext)


object BlobSourceStreamingDataProvider:
  private type Environment = BlobSourceDataProvider & VersionedDataGraphBuilderSettings & BackfillSettings & StreamContext

  def apply(
      dataProvider: BlobSourceDataProvider,
      settings: VersionedDataGraphBuilderSettings,
      backfillSettings: BackfillSettings,
      streamContext: StreamContext
  ): BlobSourceStreamingDataProvider = new BlobSourceStreamingDataProvider(dataProvider, settings, backfillSettings, streamContext)

  val layer: ZLayer[Environment, Nothing, StreamDataProvider] =
    ZLayer {
      for
        dataProvider  <- ZIO.service[BlobSourceDataProvider]
        settings      <- ZIO.service[VersionedDataGraphBuilderSettings]
        backfillSettings      <- ZIO.service[BackfillSettings]
        streamContext <- ZIO.service[StreamContext]
      yield BlobSourceStreamingDataProvider(dataProvider, settings, backfillSettings, streamContext)
    }
