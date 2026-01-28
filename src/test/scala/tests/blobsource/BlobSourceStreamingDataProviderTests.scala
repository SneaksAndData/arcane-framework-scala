package com.sneaksanddata.arcane.framework
package tests.blobsource

import models.app.StreamContext
import models.schemas.ArcaneType.StringType
import models.schemas.{ArcaneSchema, Field}
import models.settings.BackfillBehavior.Overwrite
import models.settings.{BackfillBehavior, BackfillSettings, VersionedDataGraphBuilderSettings}
import services.blobsource.providers.{BlobSourceDataProvider, BlobSourceStreamingDataProvider}
import services.blobsource.readers.listing.BlobListingParquetSource
import services.blobsource.versioning.BlobSourceWatermark
import services.iceberg.{IcebergS3CatalogWriter, IcebergTablePropertyManager, given_Conversion_ArcaneSchema_Schema}
import services.metrics.DeclaredMetrics
import services.storage.models.s3.S3StoragePath
import tests.shared.IcebergCatalogInfo.{defaultSinkSettings, defaultStagingSettings}
import tests.shared.S3StorageInfo.*
import tests.shared.{NullDimensionsProvider, TestDynamicSinkSettings}

import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, Task, ZIO}

import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}

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

  private val propertyManager: IcebergTablePropertyManager = IcebergTablePropertyManager(TestDynamicSinkSettings(backfillSettings.backfillTableFullName))
  private val writer: IcebergS3CatalogWriter = IcebergS3CatalogWriter(defaultStagingSettings)

  private def prepareWatermark(tableName: String, value: BlobSourceWatermark): Task[Unit] =
    for
      targetName <- ZIO.succeed(tableName)
      // prepare target table metadata
      watermarkTime <- ZIO.succeed(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).minusHours(3))
      _             <- writer.createTable(targetName, ArcaneSchema(Seq(Field("test", StringType))), true)
      _             <- propertyManager.comment(targetName, value.toJson)
    yield ()

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("BlobSourceStreamingDataProvider")(
    test("streams rows in backfill mode correctly") {
      for
        path   <- ZIO.succeed(S3StoragePath(s"s3a://$bucket").get)
        source <- ZIO.succeed(BlobListingParquetSource(path, storageReader, "/tmp", Seq("col0"), false, None))
        _      <- prepareWatermark("test", BlobSourceWatermark.epoch)
        dataProvider <- ZIO.succeed(
          BlobSourceDataProvider(
            source,
            propertyManager,
            new TestDynamicSinkSettings("demo.test.test"),
            streamSettings,
            backfillSettings
          )
        )
        sdp <- ZIO.succeed(
          BlobSourceStreamingDataProvider(
            dataProvider,
            streamSettings,
            backfillSettings,
            backfillStreamContext,
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
        _      <- prepareWatermark("test", BlobSourceWatermark.epoch)
        dataProvider <- ZIO.succeed(
          BlobSourceDataProvider(
            source,
            propertyManager,
            new TestDynamicSinkSettings("demo.test.test"),
            streamSettings,
            backfillSettings
          )
        )
        sdp <- ZIO.succeed(
          BlobSourceStreamingDataProvider(
            dataProvider,
            streamSettings,
            backfillSettings,
            changeCaptureStreamContext,
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
        _ <- prepareWatermark("test", BlobSourceWatermark.fromEpochSecond(Instant.now().minusSeconds(1).getEpochSecond))
        dataProvider <- ZIO.succeed(
          BlobSourceDataProvider(
            source,
            propertyManager,
            new TestDynamicSinkSettings("demo.test.test"),
            emptyStreamSettings,
            backfillSettings
          )
        )
        sdp <- ZIO.succeed(
          BlobSourceStreamingDataProvider(
            dataProvider,
            emptyStreamSettings,
            backfillSettings,
            changeCaptureStreamContext,
            DeclaredMetrics(NullDimensionsProvider)
          )
        )
        rows <- sdp.stream.timeout(zio.Duration.fromSeconds(5)).runCount
      yield assertTrue(rows == 0)
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock @@ TestAspect.sequential
