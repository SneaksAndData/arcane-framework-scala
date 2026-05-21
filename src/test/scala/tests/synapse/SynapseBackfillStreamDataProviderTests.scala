package com.sneaksanddata.arcane.framework
package tests.synapse

import models.settings.TableNaming.{getBackfillTableName, parts}
import services.backfill.DefaultBackfillStateManager
import services.metrics.DeclaredMetrics
import services.synapse.backfill.{
  SynapseBackfillSourceDataProvider,
  SynapseBackfillStreamDataProvider,
  SynapseShardFactory
}
import services.synapse.base.SynapseLinkReader
import services.synapse.versioning.SynapseWatermark
import tests.shared.TestAzureStorageInfo.{sourceRoot, storageReader}
import tests.shared.{IcebergUtil, TestDynamicSinkSettings}
import tests.synapse.SynapseLinkTestSettings.defaultStreamMode

import zio.stream.ZStream
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO}

object SynapseBackfillStreamDataProviderTests extends ZIOSpecDefault:
  private val sourceTableName     = "dimensionattributelevelvalue"
  private val icebergUtilBackfill = IcebergUtil(TestDynamicSinkSettings("test").icebergCatalog)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("SynapseBackfillStreamDataProviderTests")(
    test(
      "runs a new backfill"
    ) {
      for
        tableSinkSettings   <- ZIO.succeed(TestDynamicSinkSettings("iceberg.test.synapse_new_backfill"))
        icebergUtilBackfill <- ZIO.succeed(IcebergUtil(tableSinkSettings.icebergCatalog))
        // shaper requires target table to exist
        _ <- icebergUtilBackfill.prepareWatermark(
          tableSinkSettings.targetTableFullName.parts.name,
          SynapseWatermark.epoch
        )
        //          propertyManager <- ZIO.service[IcebergSinkTablePropertyManager]
        stagingPropertyManager <- icebergUtilBackfill.getStagingTablePropertyManager
        stagingEntityManager   <- icebergUtilBackfill.getStagingEntityManager
        backfillStateManager <- ZIO.succeed(
          new DefaultBackfillStateManager(
            stagingEntityManager,
            stagingPropertyManager,
            new SynapseShardFactory(),
            getBackfillTableName("synapse__backfill_new")
          )
        )
//            shaperBuilder <- ZIO.succeed(
//              TestThroughputShaperBuilder.default(propertyManager, tableSinkSettings)
//            )

        synapseLinkReader <- ZIO.succeed(SynapseLinkReader(storageReader, sourceTableName, sourceRoot))
        schema            <- synapseLinkReader.getSchema
        // backfill requires staging table to exist
        _ <- icebergUtilBackfill.prepareBackfillTable(getBackfillTableName("synapse__backfill_new"), schema)
        synapseLinkDataProvider <- ZIO.succeed(
          SynapseBackfillSourceDataProvider(
            synapseLinkReader,
            defaultStreamMode.backfill,
            tableSinkSettings,
            backfillStateManager,
            "backfill_new"
          )
        )
        provider <- ZIO.succeed(
          SynapseBackfillStreamDataProvider(
            synapseLinkDataProvider,
            defaultStreamMode.backfill,
            backfillStateManager,
            DeclaredMetrics()
          )
        )
        data      <- provider.backfillStream
        shards    <- data.stream.runCollect
        shardRows <- ZStream.fromIterable(shards).flatMap(_.shardStream._1).runCount
      // expect **8** shards as the source has 8 date folders that map to backfillStart, backfillEnd range
      // for all shards combined:
      // expect 42 rows, since each IU file has 5 rows and 1 row in a D file -> 6 rows per folder
      // total **7** files for this table (first folder doesn't have a CSV/schema for this table)
      // note that last batch is NOT skipped, even though it might be not processed
      // streaming mode INCLUDES current watermark folder into the data stream, so even if the current snapshot was not complete, streaming should merge it in
      yield assertTrue(shards.size == 8 && shardRows == 6 * 7)
    }
  ) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock
