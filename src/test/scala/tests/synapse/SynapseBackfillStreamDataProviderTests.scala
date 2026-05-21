package com.sneaksanddata.arcane.framework
package tests.synapse

import models.settings.TableNaming.{getBackfillTableName, parts}
import services.backfill.DefaultBackfillStateManager
import services.iceberg.IcebergSinkTablePropertyManager
import services.metrics.DeclaredMetrics
import services.synapse.backfill.{SynapseBackfillSourceDataProvider, SynapseBackfillStreamDataProvider, SynapseShardFactory}
import services.synapse.base.SynapseLinkReader
import services.synapse.versioning.SynapseWatermark
import tests.shared.TestAzureStorageInfo.{sourceRoot, storageReader}
import tests.shared.{IcebergUtil, TestDynamicSinkSettings, TestThroughputShaperBuilder}
import tests.synapse.SynapseLinkTestSettings.defaultStreamMode

import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO}

class SynapseBackfillStreamDataProviderTests extends ZIOSpecDefault:
  private val sourceTableName = "dimensionattributelevelvalue"
  
  override def spec: Spec[TestEnvironment & Scope, Any] = suite("SynapseBackfillStreamDataProviderTests")(
        test(
          "runs a new backfill"
        ) {
          for
            tableSinkSettings <- ZIO.succeed(TestDynamicSinkSettings("iceberg.test.synapse_new_backfill"))
            icebergUtilBackfill <- ZIO.succeed(IcebergUtil(tableSinkSettings.icebergCatalog))
            // shaper requires target table to exist
            _ <- icebergUtilBackfill.prepareWatermark(
              tableSinkSettings.targetTableFullName.parts.name,
              SynapseWatermark.epoch
            )
  //          propertyManager <- ZIO.service[IcebergSinkTablePropertyManager]
            stagingPropertyManager <- icebergUtilBackfill.getStagingTablePropertyManager
            stagingEntityManager <- icebergUtilBackfill.getStagingEntityManager
            backfillStateManager <- ZIO.succeed(new DefaultBackfillStateManager(
              stagingEntityManager,
              stagingPropertyManager,
              new SynapseShardFactory(),
              getBackfillTableName("synapse__backfill_new")
            ))
//            shaperBuilder <- ZIO.succeed(
//              TestThroughputShaperBuilder.default(propertyManager, tableSinkSettings)
//            )

            synapseLinkReader <- ZIO.succeed(SynapseLinkReader(storageReader, sourceTableName, sourceRoot))
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
            data <- provider.backfillStream
            shardCount <- data.stream.runCount
          // expect 30 rows, since each file has 5 rows
          // total 7 files for this table (first folder doesn't have a CSV/schema for this table)
          // 1 file skipped as it is the latest one
          // plus there 1 record to be deleted
          // plus final row must be watermark row
          yield assertTrue(shardCount == 10)
        } //.provideLayer(icebergUtilBackfill.getSinkTablePropertyManagerLayer),
  ) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock