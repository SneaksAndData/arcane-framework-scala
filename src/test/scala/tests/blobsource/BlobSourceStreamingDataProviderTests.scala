package com.sneaksanddata.arcane.framework
package tests.blobsource

import models.app.{BaseStreamContext, PluginStreamContext}
import models.settings.backfill.BackfillBehavior.Overwrite
import models.settings.backfill.{BackfillBehavior, BackfillSettings}
import models.settings.observability.ObservabilitySettings
import models.settings.sink.SinkSettings
import models.settings.sources.StreamSourceSettings
import models.settings.staging.StagingSettings
import models.settings.streaming.{ChangeCaptureSettings, StreamModeSettings, ThroughputSettings}
import services.blobsource.providers.{BlobSourceDataProvider, BlobSourceStreamingDataProvider}
import services.blobsource.readers.listing.BlobListingParquetSource
import services.blobsource.versioning.BlobSourceWatermark
import services.metrics.DeclaredMetrics
import services.storage.models.s3.S3StoragePath
import tests.shared.IcebergCatalogInfo.defaultIcebergStagingSettings
import tests.shared.S3StorageInfo.*
import tests.shared.{
  IcebergUtil,
  NullDimensionsProvider,
  TestDynamicSinkSettings,
  TestStagingSettings,
  TestThroughputShaperBuilder
}

import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO}

import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}

object BlobSourceStreamingDataProviderTests extends ZIOSpecDefault:
  private val defaultStreamMode = new StreamModeSettings {

    /** Backfill mode-only settings
      */
    override val backfill: BackfillSettings = new BackfillSettings {
      override val backfillBehavior: BackfillBehavior = Overwrite
      override val backfillStartDate: Option[OffsetDateTime] = Some(
        OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofHours(12))
      )
    }

    /** Change capture mode settings
      */
    override val changeCapture: ChangeCaptureSettings = new ChangeCaptureSettings {
      override val changeCaptureInterval: Duration     = Duration.ofSeconds(5)
      override val changeCaptureJitterVariance: Double = 0.0001
      override val changeCaptureJitterSeed: Long       = 0
    }
  }

  private val stagingSettings = TestStagingSettings()

  private val icebergUtil =
    IcebergUtil(
      TestDynamicSinkSettings(stagingSettings.table.backfillTableName).icebergCatalog
    )

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("BlobSourceStreamingDataProvider")(
    test("streams rows in backfill mode correctly") {
      for
        path   <- ZIO.succeed(S3StoragePath(s"s3a://$bucket").get)
        source <- ZIO.succeed(BlobListingParquetSource(path, storageReader, "/tmp", Seq("col0"), false, None))
        _      <- icebergUtil.prepareWatermark("test", BlobSourceWatermark.epoch)
        dataProvider <- ZIO.succeed(
          BlobSourceDataProvider(
            source,
            icebergUtil.propertyManager,
            new TestDynamicSinkSettings("demo.test.test"),
            defaultStreamMode,
            TestThroughputShaperBuilder.default(
              icebergUtil.propertyManager,
              new TestDynamicSinkSettings(s"demo.test.test")
            )
          )
        )
        sdp <- ZIO.succeed(
          BlobSourceStreamingDataProvider(
            dataProvider,
            defaultStreamMode.changeCapture,
            defaultStreamMode.backfill,
            true,
            DeclaredMetrics(NullDimensionsProvider)
          )
        )
        rows <- sdp.stream.runCollect
      yield assertTrue(rows.size == 50 * 100 + 1 && rows.last.isWatermark) // watermark must be present at the end
    },
    test("stream changes correctly") {
      for
        path   <- ZIO.succeed(S3StoragePath(s"s3a://$bucket").get)
        source <- ZIO.succeed(BlobListingParquetSource(path, storageReader, "/tmp", Seq("col0"), false, None))
        _      <- icebergUtil.prepareWatermark("test", BlobSourceWatermark.epoch)
        dataProvider <- ZIO.succeed(
          BlobSourceDataProvider(
            source,
            icebergUtil.propertyManager,
            new TestDynamicSinkSettings("demo.test.test"),
            defaultStreamMode,
            TestThroughputShaperBuilder.default(
              icebergUtil.propertyManager,
              new TestDynamicSinkSettings(s"demo.test.test")
            )
          )
        )
        sdp <- ZIO.succeed(
          BlobSourceStreamingDataProvider(
            dataProvider,
            defaultStreamMode.changeCapture,
            defaultStreamMode.backfill,
            false,
            DeclaredMetrics(NullDimensionsProvider)
          )
        )
        rows <- sdp.stream.filter(!_.isWatermark).timeout(zio.Duration.fromSeconds(10)).runCount
      // since no new files are added to the storage, emitted amount should be equal to backfill run and do not increase
      yield assertTrue(rows == 50 * 100)
    },
    test("stream changes respecting watermark") {
      for
        path   <- ZIO.succeed(S3StoragePath(s"s3a://$bucket").get)
        source <- ZIO.succeed(BlobListingParquetSource(path, storageReader, "/tmp", Seq("col0"), false, None))
        _ <- icebergUtil.prepareWatermark(
          "test",
          BlobSourceWatermark.fromEpochSecond(Instant.now().minusSeconds(1).getEpochSecond)
        )
        dataProvider <- ZIO.succeed(
          BlobSourceDataProvider(
            source,
            icebergUtil.propertyManager,
            new TestDynamicSinkSettings("demo.test.test"),
            defaultStreamMode,
            TestThroughputShaperBuilder.default(
              icebergUtil.propertyManager,
              new TestDynamicSinkSettings(s"demo.test.test")
            )
          )
        )
        sdp <- ZIO.succeed(
          BlobSourceStreamingDataProvider(
            dataProvider,
            defaultStreamMode.changeCapture,
            defaultStreamMode.backfill,
            false,
            DeclaredMetrics(NullDimensionsProvider)
          )
        )
        rows <- sdp.stream.timeout(zio.Duration.fromSeconds(5)).runCount
      yield assertTrue(rows == 0)
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock @@ TestAspect.sequential
