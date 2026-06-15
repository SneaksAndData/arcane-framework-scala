package com.sneaksanddata.arcane.framework
package tests.synapse

import models.backfill.DefaultSourceBackfill
import models.settings.TableNaming.parts
import services.backfill.DefaultBackfillStateManager
import services.metrics.DeclaredMetrics
import services.naming.DefaultNameGenerator
import services.synapse.backfill.{
  SynapseBackfillSourceDataProvider,
  SynapseShardFactory,
  SynapseShardedBackfillStreamDataProvider
}
import services.synapse.base.SynapseLinkStreamingSource
import services.synapse.versioning.SynapseWatermark
import tests.shared.TestAzureStorageInfo.{sourceRoot, storageReader}
import tests.shared.{IcebergUtil, TestDynamicSinkSettings, TestSourceBufferingSettings, TestThroughputShaperBuilder}
import tests.synapse.SynapseLinkTestSettings.defaultStreamMode

import zio.stream.ZStream
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO}

import java.time.OffsetDateTime

object SynapseBackfillStreamDataProviderTests extends ZIOSpecDefault:
  private val sourceTableName     = "dimensionattributelevelvalue"
  private val icebergUtilBackfill = IcebergUtil(TestDynamicSinkSettings("test").icebergCatalog)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("SynapseBackfillStreamDataProviderTests")(
    test(
      "streams correct number of shards and rows"
    ) {
      for
        tableSinkSettings <- ZIO.succeed(TestDynamicSinkSettings("iceberg.test.synapse_new_backfill"))
        nameGenerator <- ZIO.succeed(
          new DefaultNameGenerator(
            sinkSettings = tableSinkSettings,
            backfillId = "backfill_new",
            streamId = "synapse-backfill-stream-data-provider-tests"
          )
        )
        icebergUtilBackfill <- ZIO.succeed(IcebergUtil(tableSinkSettings.icebergCatalog))
        // shaper requires target table to exist
        _ <- icebergUtilBackfill.prepareWatermark(
          tableSinkSettings.targetTableFullName.parts.name,
          SynapseWatermark.epoch
        )
        propertyManager        <- icebergUtilBackfill.getSinkTablePropertyManager
        stagingPropertyManager <- icebergUtilBackfill.getStagingTablePropertyManager
        stagingEntityManager   <- icebergUtilBackfill.getStagingEntityManager
        backfillStateManager <- ZIO.succeed(
          new DefaultBackfillStateManager(
            stagingEntityManager,
            stagingPropertyManager,
            new SynapseShardFactory(nameGenerator),
            nameGenerator,
            DeclaredMetrics()
          )
        )
        shaperBuilder <- ZIO.succeed(
          TestThroughputShaperBuilder.default(propertyManager, tableSinkSettings)
        )

        synapseLinkReader <- ZIO.succeed(SynapseLinkStreamingSource(storageReader, sourceTableName, sourceRoot))
        schema            <- synapseLinkReader.getSchema
        backfillTableName <- nameGenerator.getBackfillTableName
        // backfill requires staging table to exist
        _ <- icebergUtilBackfill.prepareBackfillTable(backfillTableName, schema)
        synapseLinkDataProvider <- ZIO.succeed(
          SynapseBackfillSourceDataProvider(
            synapseLinkReader,
            defaultStreamMode.backfill,
            backfillStateManager,
            shaperBuilder,
            TestSourceBufferingSettings,
            nameGenerator,
            "backfill_new"
          )
        )
        provider <- ZIO.succeed(
          SynapseShardedBackfillStreamDataProvider(
            synapseLinkDataProvider,
            defaultStreamMode.backfill,
            backfillStateManager,
            DeclaredMetrics()
          )
        )
        data      <- provider.backfillStream
        shards    <- data.stream.runCollect
        shardRows <- ZStream.fromIterable(shards).flatMap(_.shardStream._1).runCount
        backfillState <- stagingPropertyManager
          .getRequiredProperty(backfillTableName, "backfill")
          .map(upickle.read[DefaultSourceBackfill](_))
      // expect **8** shards as the source has 8 date folders that map to backfillStart, backfillEnd range
      // for all shards combined:
      // expect 42 rows, since each IU file has 5 rows and 1 row in a D file -> 6 rows per folder
      // total **7** files for this table (first folder doesn't have a CSV/schema for this table)
      // note that last batch is NOT skipped, even though it might be not processed
      // streaming mode INCLUDES current watermark folder into the data stream, so even if the current snapshot was not complete, streaming should merge it in
      yield assertTrue(shards.size == 8 && shardRows == 6 * 7 && backfillState.id == "backfill_new")
    },
    test(
      "resumes an interrupted backfill"
    ) {
      for
        tableSinkSettings <- ZIO.succeed(TestDynamicSinkSettings("iceberg.test.synapse_interrupted_backfill"))
        nameGenerator <- ZIO.succeed(
          new DefaultNameGenerator(
            sinkSettings = tableSinkSettings,
            backfillId = "backfill_interrupted",
            streamId = "synapse-backfill-stream-data-provider-tests"
          )
        )
        icebergUtilBackfill <- ZIO.succeed(IcebergUtil(tableSinkSettings.icebergCatalog))
        // shaper requires target table to exist
        _ <- icebergUtilBackfill.prepareWatermark(
          tableSinkSettings.targetTableFullName.parts.name,
          SynapseWatermark.epoch
        )
        propertyManager        <- icebergUtilBackfill.getSinkTablePropertyManager
        stagingPropertyManager <- icebergUtilBackfill.getStagingTablePropertyManager
        stagingEntityManager   <- icebergUtilBackfill.getStagingEntityManager
        backfillStateManager <- ZIO.succeed(
          new DefaultBackfillStateManager(
            stagingEntityManager,
            stagingPropertyManager,
            new SynapseShardFactory(nameGenerator),
            nameGenerator,
            DeclaredMetrics()
          )
        )
        shaperBuilder <- ZIO.succeed(
          TestThroughputShaperBuilder.default(propertyManager, tableSinkSettings)
        )
        backfillTableName <- nameGenerator.getBackfillTableName

        synapseLinkReader <- ZIO.succeed(SynapseLinkStreamingSource(storageReader, sourceTableName, sourceRoot))
        folders <- storageReader
          .streamPrefixes(sourceRoot)
          .runCollect
          .map(_.filterNot(_.name.toLowerCase.contains("changelog")).filterNot(_.name.contains("model")))
        schema <- synapseLinkReader.getSchema
        // backfill requires staging table to exist
        _ <- icebergUtilBackfill.prepareBackfillTable(
          backfillTableName,
          schema,
          Some(
            upickle.write(
              DefaultSourceBackfill(
                id = "backfill_interrupted",
                backfillStart = SynapseWatermark(
                  version = "2025-01-01T00:00:00Z",
                  timestamp = OffsetDateTime.now(),
                  prefix = "2025-01-01T00:00:00Z"
                ).toJson,
                backfillEnd = SynapseWatermark(
                  version = "2025-01-02T00:00:00Z",
                  timestamp = OffsetDateTime.now(),
                  prefix = "2025-01-02T00:00:00Z"
                ).toJson,
                shardSources = folders.map(_.name).take(4)
              )
            )
          )
        )
        synapseLinkDataProvider <- ZIO.succeed(
          SynapseBackfillSourceDataProvider(
            synapseLinkReader,
            defaultStreamMode.backfill,
            backfillStateManager,
            shaperBuilder,
            TestSourceBufferingSettings,
            nameGenerator,
            "backfill_interrupted"
          )
        )
        provider <- ZIO.succeed(
          SynapseShardedBackfillStreamDataProvider(
            synapseLinkDataProvider,
            defaultStreamMode.backfill,
            backfillStateManager,
            DeclaredMetrics()
          )
        )
        data      <- provider.backfillStream
        shards    <- data.stream.runCollect
        shardRows <- ZStream.fromIterable(shards).flatMap(_.shardStream._1).runCount
        backfillState <- stagingPropertyManager
          .getRequiredProperty(backfillTableName, "backfill")
          .map(upickle.read[DefaultSourceBackfill](_))
      // expect **4** shards since metadata claims 4 were staged
      // for all shards combined:
      // expect 18 rows, since each IU file has 5 rows and 1 row in a D file -> 6 rows per folder, first folder doesn't have a matching file
      // total **3** files for this table
      // note that last batch is NOT skipped, even though it might be not processed
      // streaming mode INCLUDES current watermark folder into the data stream, so even if the current snapshot was not complete, streaming should merge it in
      yield assertTrue(shards.size == 4 && shardRows == 6 * 3 && backfillState.id == "backfill_interrupted")
    }
  ) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock
