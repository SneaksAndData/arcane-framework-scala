package com.sneaksanddata.arcane.framework
package tests.synapse

import zio.{Cause, Scope, ZIO}
import zio.test.*
import zio.test.TestAspect.timeout

import java.time.{Duration, OffsetDateTime}
import tests.synapse.SynapseLinkStorageInfo.*
import models.settings.{BackfillBehavior, BackfillSettings, VersionedDataGraphBuilderSettings}
import services.storage.models.azure.AdlsStoragePath
import services.synapse.SynapseLinkStreamingDataProvider
import services.synapse.base.{SynapseLinkDataProvider, SynapseLinkReader}

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.BackfillBehavior.Overwrite

object SynapseLinkStreamingDataProviderTests extends ZIOSpecDefault:
  private val tableName = "dimensionattributelevelvalue"
  private val graphSettings = new VersionedDataGraphBuilderSettings {
    override val lookBackInterval: Duration = Duration.ofHours(2)
    override val changeCaptureInterval: Duration = Duration.ofSeconds(5)
    override val changeCapturePeriod: Duration = Duration.ofHours(1)
  }
  private val backfillSettings = new BackfillSettings {
    override val backfillBehavior: BackfillBehavior = Overwrite
    override val backfillStartDate: Option[OffsetDateTime] = Some(OffsetDateTime.now().minus(Duration.ofHours(12)))
    override val backfillTableFullName: String = "backfill_test"
  }
  private val streamContext = new StreamContext {
    override def IsBackfilling: Boolean = true

    override def streamId: String = "test"

    override def streamKind: String = "units"
  }

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("SynapseLinkStreamingDataProvider") (
    test("streams rows from the specified entity correctly") {
      for
        path <- ZIO.succeed(AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get)
        synapseLinkReader <- ZIO.succeed(SynapseLinkReader(storageReader, tableName, path))
        synapseLinkDataProvider <- ZIO.succeed(SynapseLinkDataProvider(synapseLinkReader, graphSettings, backfillSettings))
        provider <- ZIO.succeed(SynapseLinkStreamingDataProvider(synapseLinkDataProvider, graphSettings, streamContext))
        rows <- provider.stream.map(_ => 1).runSum
      // expect 30 rows, since each file has 5 rows
      // total 7 files for this table (first folder doesn't have a CSV/schema for this table) 
      // 1 file skipped as it is the latest one
      yield assertTrue(rows == 5 * (7 - 1))
    }
  ) @@ timeout(zio.Duration.fromSeconds(10)) @@ TestAspect.withLiveClock
