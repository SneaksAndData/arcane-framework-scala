package com.sneaksanddata.arcane.framework
package tests.synapse

import models.app.StreamContext
import models.schemas.{DataRow, MergeKeyField}
import models.settings.BackfillBehavior.Overwrite
import models.settings.{BackfillBehavior, BackfillSettings, VersionedDataGraphBuilderSettings}
import services.metrics.DeclaredMetrics
import services.storage.models.azure.AdlsStoragePath
import services.synapse.SynapseLinkStreamingDataProvider
import services.synapse.base.{SynapseLinkDataProvider, SynapseLinkReader}
import tests.shared.AzureStorageInfo.*
import tests.shared.NullDimensionsProvider

import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO}

import java.time.{Duration, OffsetDateTime, ZoneOffset}

object SynapseLinkStreamingDataProviderTests extends ZIOSpecDefault:
  private val tableName = "dimensionattributelevelvalue"
  private val graphSettings = new VersionedDataGraphBuilderSettings {
    override val lookBackInterval: Duration      = Duration.ofHours(3)
    override val changeCaptureInterval: Duration = Duration.ofSeconds(5)
  }
  private val backfillSettings = new BackfillSettings {
    override val backfillBehavior: BackfillBehavior = Overwrite
    override val backfillStartDate: Option[OffsetDateTime] = Some(
      OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofHours(12))
    )
    override val backfillTableFullName: String = "backfill_test"
  }
  private val backfillStreamContext = new StreamContext {
    override def IsBackfilling: Boolean = true

    override def streamId: String = "test"

    override def streamKind: String = "units"
  }
  private val changeCaptureStreamContext = new StreamContext {
    override def IsBackfilling: Boolean = false

    override def streamId: String = "test"

    override def streamKind: String = "units"
  }

  private def isDelete(row: DataRow): Boolean = row.exists(c => c.name == "IsDelete" && c.value == true)

  private def getKey(row: DataRow): String = row.find(c => c.name == MergeKeyField.name).get.value.toString
  private def reduceRows(rows: List[Option[(DataRow, Int)]]): Option[DataRow] = rows
    .reduce { case (rowA, rowB) =>
      (rowA, rowB) match
        case (None, None)    => None
        case (None, Some(_)) => rowB
        case (Some(_), None) => rowA
        case (Some(a), Some(b)) =>
          (isDelete(a._1), isDelete(b._1)) match
            case (false, false) => rowA
            case (true, false)  => if a._2 > b._2 then None else rowA
            case (false, true)  => if b._2 > a._2 then None else rowB
            case _              => None
    }
    .map(_._1)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("SynapseLinkStreamingDataProvider")(
    test("streams rows in backfill mode correctly") {
      for
        path <- ZIO.succeed(AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get)
        synapseLinkReader <- ZIO.succeed(SynapseLinkReader(storageReader, tableName, path))
        synapseLinkDataProvider <- ZIO.succeed(
          SynapseLinkDataProvider(synapseLinkReader, graphSettings, backfillSettings)
        )
        provider <- ZIO.succeed(
          SynapseLinkStreamingDataProvider(
            synapseLinkDataProvider,
            graphSettings,
            backfillStreamContext,
            DeclaredMetrics(NullDimensionsProvider)
          )
        )
        rows <- provider.stream.runCollect()
      // expect 30 rows, since each file has 5 rows
      // total 7 files for this table (first folder doesn't have a CSV/schema for this table)
      // 1 file skipped as it is the latest one
      // plus there 1 record to be deleted
      yield assertTrue(rows.size == 5 * (7 - 1) + 1 * (7 - 1))
    },
    test("stream correct number of changes") {
      for
        path <- ZIO.succeed(AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get)
        synapseLinkReader <- ZIO.succeed(SynapseLinkReader(storageReader, tableName, path))
        synapseLinkDataProvider <- ZIO.succeed(
          SynapseLinkDataProvider(synapseLinkReader, graphSettings, backfillSettings)
        )
        provider <- ZIO.succeed(
          SynapseLinkStreamingDataProvider(
            synapseLinkDataProvider,
            graphSettings,
            changeCaptureStreamContext,
            DeclaredMetrics(NullDimensionsProvider)
          )
        )
        rows <- provider.stream.timeout(zio.Duration.fromSeconds(2)).runCount
      // expect 5 rows, since each file has 5 rows
      // total 7 files for this table (first folder doesn't have a CSV/schema for this table)
      // lookback is 3 hours which should only capture 1 file
      yield assertTrue(rows == 5 + 1)
    },
    test("stream changes in the correct order") {
      for
        path <- ZIO.succeed(AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get)
        synapseLinkReader <- ZIO.succeed(SynapseLinkReader(storageReader, tableName, path))
        synapseLinkDataProvider <- ZIO.succeed(
          SynapseLinkDataProvider(synapseLinkReader, graphSettings, backfillSettings)
        )
        provider <- ZIO.succeed(
          SynapseLinkStreamingDataProvider(
            synapseLinkDataProvider,
            graphSettings,
            changeCaptureStreamContext,
            DeclaredMetrics(NullDimensionsProvider)
          )
        )
        rows <- provider.stream.timeout(zio.Duration.fromSeconds(2)).runCollect
      // delete must ALWAYS come last, otherwise there is a risk of re-inserting the same row
      yield assertTrue(rows.toList.zipWithIndex.filter(r => isDelete(r._1)).head._2 == 5) &&
        assertTrue(
          rows.toList.zipWithIndex
            .map(v => (getKey(v._1), Option(v)))
            .groupBy(v => v._1)
            .map(v => reduceRows(v._2.map(_._2)))
            .count(_.nonEmpty) == 4
        )
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock @@ TestAspect.sequential
