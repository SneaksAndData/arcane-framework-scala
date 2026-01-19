package com.sneaksanddata.arcane.framework
package tests.blobsource

import models.app.StreamContext
import models.settings.BackfillBehavior.Overwrite
import models.settings.{BackfillBehavior, BackfillSettings, VersionedDataGraphBuilderSettings}
import services.blobsource.providers.{BlobSourceDataProvider, BlobSourceStreamingDataProvider}
import services.blobsource.readers.listing.BlobListingParquetSource
import services.storage.models.s3.S3StoragePath
import tests.shared.S3StorageInfo.*

import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO}

import java.time.{Duration, OffsetDateTime, ZoneOffset}

object BlobSourceStreamingDataProviderTests extends ZIOSpecDefault:
  private val streamSettings = new VersionedDataGraphBuilderSettings {
    override val lookBackInterval: Duration      = Duration.ofHours(3)
    override val changeCaptureInterval: Duration = Duration.ofSeconds(5)
  }

  private val emptyStreamSettings = new VersionedDataGraphBuilderSettings {
    override val lookBackInterval: Duration      = Duration.ofSeconds(1)
    override val changeCaptureInterval: Duration = Duration.ofSeconds(5)
  }

  private val backfillSettings = new BackfillSettings {
    override val backfillBehavior: BackfillBehavior = Overwrite
    override val backfillStartDate: Option[OffsetDateTime] = Some(
      OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofHours(12))
    )
    override val backfillTableFullName: String = "blobsource_backfill_test"
  }

  private val backfillStreamContext = new StreamContext {
    override def IsBackfilling: Boolean = true

    override def streamId: String = "blob-source"

    override def streamKind: String = "units"
  }
  private val changeCaptureStreamContext = new StreamContext {
    override def IsBackfilling: Boolean = false

    override def streamId: String = "blob-source"

    override def streamKind: String = "units"
  }

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("BlobSourceStreamingDataProvider")(
    test("streams rows in backfill mode correctly") {
      for
        path         <- ZIO.succeed(S3StoragePath(s"s3a://$bucket").get)
        source       <- ZIO.succeed(BlobListingParquetSource(path, storageReader, "/tmp", Seq("col0"), false))
        dataProvider <- ZIO.succeed(BlobSourceDataProvider(source, streamSettings, backfillSettings))
        sdp  <- ZIO.succeed(BlobSourceStreamingDataProvider(dataProvider, streamSettings, backfillSettings, backfillStreamContext))
        rows <- sdp.stream.runCount
      yield assertTrue(rows == 50 * 100)
    },
    test("stream changes correctly") {
      for
        path         <- ZIO.succeed(S3StoragePath(s"s3a://$bucket").get)
        source       <- ZIO.succeed(BlobListingParquetSource(path, storageReader, "/tmp", Seq("col0"), false))
        dataProvider <- ZIO.succeed(BlobSourceDataProvider(source, streamSettings, backfillSettings))
        sdp  <- ZIO.succeed(BlobSourceStreamingDataProvider(dataProvider, streamSettings, backfillSettings, changeCaptureStreamContext))
        rows <- sdp.stream.timeout(zio.Duration.fromSeconds(10)).runCount
      // since no new files are added to the storage, emitted amount should be equal to backfill run and do not increase
      yield assertTrue(rows == 50 * 100)
    },
    test("stream changes respecting lookback interval") {
      for
        path         <- ZIO.succeed(S3StoragePath(s"s3a://$bucket").get)
        source       <- ZIO.succeed(BlobListingParquetSource(path, storageReader, "/tmp", Seq("col0"), false))
        dataProvider <- ZIO.succeed(BlobSourceDataProvider(source, emptyStreamSettings, backfillSettings))
        sdp <- ZIO.succeed(
          BlobSourceStreamingDataProvider(dataProvider, emptyStreamSettings, backfillSettings, changeCaptureStreamContext)
        )
        rows <- sdp.stream.timeout(zio.Duration.fromSeconds(5)).runCount
      yield assertTrue(rows == 0)
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock @@ TestAspect.sequential
