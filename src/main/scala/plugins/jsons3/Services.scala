package com.sneaksanddata.arcane.framework
package plugins.jsons3

import models.app.PluginStreamContext
import plugins.{BlobListProvidedServices, BlobListRequiredServices}
import services.backfill.DefaultBackfillStateManager
import services.blobsource.backfill.{
  BlobBackfillSourceDataProvider,
  BlobShardedBackfillStreamDataProvider,
  BlobSourceBackfillMergeStreamDataProvider,
  BlobSourceShardFactory
}
import services.blobsource.providers.{BlobSourceDataProvider, BlobSourceStreamingDataProvider}
import services.blobsource.readers.listing.BlobListingJsonStreamingSource
import services.blobsource.versioning.UpsertBlobStagedBatchFactory
import services.storage.models.s3.S3StoragePath
import services.streaming.throughput.base.ThroughputShaperBuilder

import zio.ZLayer

object Services:
  lazy val sourceLayer: ZLayer[
    BlobListRequiredServices & BlobListingJsonStreamingSource[S3StoragePath] & PluginStreamContext,
    Throwable,
    BlobListProvidedServices
  ] = ZLayer.makeSome[
    BlobListRequiredServices & BlobListingJsonStreamingSource[S3StoragePath] & PluginStreamContext,
    BlobListProvidedServices
  ](
    BlobSourceDataProvider.layer,
    BlobSourceStreamingDataProvider.layer,
    UpsertBlobStagedBatchFactory.layer,

    // backfill
    BlobBackfillSourceDataProvider.layer,
    BlobSourceShardFactory.layer,
    BlobShardedBackfillStreamDataProvider.layer,
    BlobSourceBackfillMergeStreamDataProvider.layer,
    ThroughputShaperBuilder.layer,
    DefaultBackfillStateManager.layer
  )
