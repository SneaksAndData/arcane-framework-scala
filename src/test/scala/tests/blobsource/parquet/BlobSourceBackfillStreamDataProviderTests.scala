package com.sneaksanddata.arcane.framework
package tests.blobsource.parquet

import models.backfill.DefaultSourceBackfill
import models.schemas.ArcaneType.{LongType, StringType, StructType}
import models.schemas.{ArcaneSchema, IndexedField, IndexedMergeKeyField}
import models.settings.TableNaming.parts
import models.settings.backfill.BackfillBehavior.Overwrite
import models.settings.backfill.{BackfillBehavior, BackfillSettings}
import models.settings.sources.{BufferingStrategy, SourceBufferingSettings, Unbounded, UnboundedImpl}
import services.backfill.DefaultBackfillStateManager
import services.blobsource.backfill.{
  BlobBackfillSourceDataProvider,
  BlobShardedBackfillStreamDataProvider,
  BlobSourceShardFactory
}
import services.blobsource.readers.listing.BlobListingParquetStreamingSource
import services.blobsource.versioning.BlobSourceWatermark
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
  private val targetSchema: ArcaneSchema = ArcaneSchema(
    Seq(
      IndexedMergeKeyField(1),
      IndexedField("col0", LongType, 2),
      IndexedField("col1", StringType, 3),
      IndexedField("col2", LongType, 4),
      IndexedField("col3", StringType, 5),
      IndexedField("col4", LongType, 6),
      IndexedField("col5", StringType, 7),
      IndexedField("col6", LongType, 8),
      IndexedField("col7", StringType, 9),
      IndexedField("col8", LongType, 10),
      IndexedField(
        "col9",
        StructType(
          ArcaneSchema(
            Seq(
              IndexedField("nested_col_0", LongType, 11),
              IndexedField("nested_col_1", StringType, 12),
              IndexedField("nested_col_2", LongType, 13),
              IndexedField("nested_col_3", StringType, 14),
              IndexedField("nested_col_4", LongType, 15)
            )
          )
        ),
        16
      ),
      IndexedField(
        "col10",
        StructType(
          ArcaneSchema(
            Seq(
              IndexedField("nested_col_0", LongType, 17),
              IndexedField("nested_col_1", StringType, 18),
              IndexedField("nested_col_2", LongType, 19),
              IndexedField("nested_col_3", StringType, 20),
              IndexedField("nested_col_4", LongType, 21)
            )
          )
        ),
        22
      )
    )
  )

  private def runBackfill(targetName: String) = for
    path              <- ZIO.succeed(S3StoragePath(s"s3a://$bucket").get)
    shardPath <- ZIO.succeed(S3StoragePath("s3a://tmp").get)
    backfillId        <- ZIO.succeed(Random.alphanumeric.take(10).mkString("").toLowerCase)
    tableSinkSettings <- ZIO.succeed(TestDynamicSinkSettings(s"iceberg.test.${targetName.replace("-", "_")}"))

    nameGenerator <- ZIO.succeed(
      new DefaultNameGenerator(
        sinkSettings = tableSinkSettings,
        backfillId = backfillId,
        streamId = targetName
      )
    )
    backfillTableName <- nameGenerator.getBackfillTableName
    _ <- icebergUtilBackfill.prepareBackfillTable(
      backfillTableName,
      targetSchema
    )
    // for shaper
    _ <- icebergUtilBackfill.prepareWatermark(
      tableSinkSettings.targetTableFullName.parts.name,
      BlobSourceWatermark.epoch
    )

    propertyManager        <- icebergUtilBackfill.getSinkTablePropertyManager
    stagingPropertyManager <- icebergUtilBackfill.getStagingTablePropertyManager
    stagingEntityManager   <- icebergUtilBackfill.getStagingEntityManager
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
      BlobListingParquetStreamingSource(
        sourcePath = path,
        shardStoragePath = shardPath,
        storageClient = storageReader,
        nameGenerator = nameGenerator,
        tempStoragePath = "/tmp",
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
          override val bufferingEnabled: Boolean            = false
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
  yield (provider, stagingPropertyManager, backfillTableName, backfillId)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("BlobSourceBackfillStreamDataProviderTests")(
    test("streams correct number of shards and rows") {
      for
        (provider, stagingPropertyManager, backfillTableName, backfillId) <- runBackfill(
          "blob-source-backfill-sdp-test-once"
        )
        data      <- provider.backfillStream
        shards    <- data.stream.runCollect
        shardRows <- ZStream.fromIterable(shards).flatMap(_.shardStream._1).runCount
        backfillState <- stagingPropertyManager
          .getRequiredProperty(backfillTableName, "backfill")
          .map(upickle.read[DefaultSourceBackfill](_))
      // 50 files gives 1 shard since we chunk into 1000 files per shard
      // row count must match source 50 * 100
      yield assertTrue(shards.size == 1 && shardRows == 5000 && backfillState.id == backfillId)
    },
    test("resumes an interrupted backfill") {
      for
        (provider, stagingPropertyManager, backfillTableName, backfillId) <- runBackfill(
          "blob-source-backfill-sdp-test-repeat"
        )
        data      <- provider.backfillStream
        shards    <- data.stream.runCollect
        shardRows <- ZStream.fromIterable(shards).flatMap(_.shardStream._1).runCount
        backfillState <- stagingPropertyManager
          .getRequiredProperty(backfillTableName, "backfill")
          .map(upickle.read[DefaultSourceBackfill](_))
      // 50 files gives 1 shard since we chunk into 1000 files per shard
      // row count must match source 50 * 100
      yield assertTrue(shards.size == 1 && shardRows == 5000 && backfillState.id == backfillId)
    } @@ TestAspect.repeats(1)
  ) @@ timeout(zio.Duration.fromSeconds(180)) @@ TestAspect.withLiveClock
