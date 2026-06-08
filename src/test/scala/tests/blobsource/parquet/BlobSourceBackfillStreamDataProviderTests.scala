package com.sneaksanddata.arcane.framework
package tests.blobsource.parquet

import models.backfill.DefaultSourceBackfill
import models.schemas.ArcaneSchema
import models.settings.backfill.BackfillBehavior.Overwrite
import models.settings.backfill.{BackfillBehavior, BackfillSettings}
import models.settings.sources.{BufferingStrategy, SourceBufferingSettings, Unbounded, UnboundedImpl}
import services.backfill.DefaultBackfillStateManager
import services.blobsource.backfill.{BlobBackfillSourceDataProvider, BlobShardedBackfillStreamDataProvider, BlobSourceShardFactory}
import services.blobsource.readers.listing.BlobListingParquetSource
import services.metrics.DeclaredMetrics
import services.naming.DefaultNameGenerator
import services.storage.models.s3.S3StoragePath
import tests.shared.S3StorageInfo.{bucket, storageReader}
import tests.shared.{IcebergUtil, TestDynamicSinkSettings, TestThroughputShaperBuilder}

import zio.internal.stacktracer.SourceLocation
import zio.stream.ZStream
import zio.test.TestAspect.timeout
import zio.test.{Spec, SuiteConstructor, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Scope, ZIO}

import java.time.{Duration, OffsetDateTime, ZoneOffset}
import scala.util.Random

object BlobSourceBackfillStreamDataProviderTests extends ZIOSpecDefault:
  private val icebergUtilBackfill = IcebergUtil(TestDynamicSinkSettings("test").icebergCatalog)
  private val targetSchema: ArcaneSchema = ???
  
  override def spec: Spec[TestEnvironment & Scope, Any] = suite("BlobSourceBackfillStreamDataProviderTests")(
    test("streams correct number of shards and rows") {
      for
        path            <- ZIO.succeed(S3StoragePath(s"s3a://$bucket").get)
        backfillId <- ZIO.succeed(Random.alphanumeric.take(10).mkString("").toLowerCase)
        tableSinkSettings <- ZIO.succeed(TestDynamicSinkSettings("iceberg.test.blobsource_backfill_sdp_tests"))
        
        nameGenerator <- ZIO.succeed(
          new DefaultNameGenerator(
            sinkSettings = tableSinkSettings,
            backfillId = backfillId,
            streamId = "blobsource_backfill_sdp_tests"
          )
        )
        backfillTableName <- nameGenerator.getBackfillTableName
        _               <- icebergUtilBackfill.prepareBackfillTable(
          backfillTableName,
          targetSchema,
        )

        propertyManager <- icebergUtilBackfill.getSinkTablePropertyManager
        stagingPropertyManager <- icebergUtilBackfill.getStagingTablePropertyManager
        stagingEntityManager <- icebergUtilBackfill.getStagingEntityManager
        backfillStateManager <- ZIO.succeed(
          new DefaultBackfillStateManager(
            stagingEntityManager,
            stagingPropertyManager,
            new BlobSourceShardFactory(nameGenerator),
            nameGenerator,
            DeclaredMetrics()
          )
        )
        shaperBuilder <- ZIO.succeed(
          TestThroughputShaperBuilder.default(propertyManager, tableSinkSettings)
        )
        reader <- ZIO.succeed(
          BlobListingParquetSource(
            sourcePath = path,
            s3Reader = storageReader,
            tempPath = "/tmp",
            primaryKeys = Seq("col0"), 
            useNameMapping = false, 
            sourceSchema = None
          )
        )
        backfillSettings <- ZIO.succeed(
          new BackfillSettings {
            override val backfillStartDate: Option[OffsetDateTime] = Some(
              OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofHours(12))
            )
            override val backfillBehavior: BackfillBehavior = Overwrite
          }
        )
        dataProvider <- ZIO.succeed(
          new BlobBackfillSourceDataProvider(
            dataProvider = reader, 
            backfillSettings = backfillSettings, 
            stateManager = backfillStateManager, 
            throughputShaperBuilder = shaperBuilder, 
            sourceBufferingSettings = new SourceBufferingSettings {
              override val bufferingStrategy: BufferingStrategy = UnboundedImpl(Unbounded())
              override val bufferingEnabled: Boolean = false
            }, 
            nameGenerator = nameGenerator, 
            backfillId = backfillId
          )
        )
        provider <- ZIO.succeed(
          new BlobShardedBackfillStreamDataProvider(
            dataProvider,
            backfillSettings,
            backfillStateManager,
            DeclaredMetrics()
          )
        )
        data <- provider.backfillStream
        shards <- data.stream.runCollect
        shardRows <- ZStream.fromIterable(shards).flatMap(_.shardStream._1).runCount
        backfillState <- stagingPropertyManager
          .getRequiredProperty(backfillTableName, "backfill")
          .map(upickle.read[DefaultSourceBackfill](_))
      // 10000 rows should result in [14, 15] shards assuming the cost input for the scaler evaluates to 2.6
      // row count must match source
      yield assertTrue(Seq(14, 15).contains(shards.size) && shardRows == 10000 && backfillState.id == backfillId)

    }
  ) @@ timeout(zio.Duration.fromSeconds(180)) @@ TestAspect.withLiveClock
