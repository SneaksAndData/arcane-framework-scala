package com.sneaksanddata.arcane.framework
package tests.synapse

import models.app.StreamContext
import models.schemas.ArcaneType.StringType
import models.schemas.{ArcaneSchema, DataRow, Field, MergeKeyField}
import models.settings.*
import models.settings.backfill.BackfillBehavior.Overwrite
import models.settings.backfill.{BackfillBehavior, BackfillSettings}
import services.iceberg.{IcebergS3CatalogWriter, IcebergTablePropertyManager, given_Conversion_ArcaneSchema_Schema}
import services.metrics.DeclaredMetrics
import services.storage.models.azure.AdlsStoragePath
import services.streaming.throughput.{MemoryBoundShaper, StaticShaper}
import services.synapse.SynapseAzureBlobReaderExtensions.asWatermark
import services.synapse.SynapseLinkStreamingDataProvider
import services.synapse.base.{SynapseLinkDataProvider, SynapseLinkReader}
import tests.shared.AzureStorageInfo.*
import tests.shared.IcebergCatalogInfo.defaultStagingSettings
import tests.shared.{NullDimensionsProvider, TestDynamicSinkSettings, TestSinkSettings}

import com.sneaksanddata.arcane.framework.models.settings.streaming.ThroughputShaperImpl.MemoryBound
import com.sneaksanddata.arcane.framework.models.settings.streaming.{ThroughputSettings, ThroughputShaperImpl}
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, Task, ZIO}

import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}

object SynapseLinkStreamingDataProviderTests extends ZIOSpecDefault:
  private val sourceTableName = "dimensionattributelevelvalue"
  private val graphSettings = new VersionedDataGraphBuilderSettings {
    override val changeCaptureInterval: Duration = Duration.ofSeconds(5)
  }
  private val backfillSettings = new BackfillSettings {
    override val backfillBehavior: BackfillBehavior = Overwrite
    override val backfillStartDate: Option[OffsetDateTime] = Some(
      OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofHours(12))
    )
    override val backfillTableFullName: String = "demo.test.synapse_backfill_test"
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

  private val writer: IcebergS3CatalogWriter = IcebergS3CatalogWriter(defaultStagingSettings)

  private val sourceRoot = AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get

  private def prepareWatermark(tableName: String, propertyManager: IcebergTablePropertyManager): Task[Unit] =
    for
      targetName <- ZIO.succeed(tableName)
      // prepare target table metadata
      watermarkTime <- ZIO.succeed(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).minusHours(3))
      _             <- writer.createTable(targetName, ArcaneSchema(Seq(Field("test", StringType))), true)
      azPrefixes    <- storageReader.streamPrefixes(sourceRoot + s"${watermarkTime.getYear}-").runCollect
      _             <- propertyManager.comment(targetName, azPrefixes.init.last.asWatermark.toJson)
    yield ()

  case object TestThroughputSettings extends ThroughputSettings:
    override val shaperImpl: ThroughputShaperImpl = MemoryBound(50, 256, 2, 2, 1, 1000, 0.5, 0.5, 2)
    override val advisedChunkSize: Int            = 1
    override val advisedRateChunks: Int           = 1
    override val advisedRatePeriod: Duration      = Duration.ofSeconds(1)
    override val advisedChunksBurst: Int          = 10

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("SynapseLinkStreamingDataProvider")(
    test(
      "streams rows in backfill mode correctly"
    ) { // backfill should not attempt to load table watermark, thus we do not need the target table to exist
      for
        tableSinkSettings <- ZIO.succeed(TestDynamicSinkSettings(backfillSettings.backfillTableFullName))
        propertyManager   <- ZIO.succeed(IcebergTablePropertyManager(tableSinkSettings))
        _                 <- prepareWatermark(tableSinkSettings.targetTableNameParts.Name, propertyManager)
        shaper <- ZIO.succeed(
          MemoryBoundShaper(
            propertyManager,
            tableSinkSettings,
            new ThroughputSettings {
              override val shaperImpl: ThroughputShaperImpl = MemoryBound(50, 256, 2, 2, 1, 1000, 0.5, 0.5, 2)
              override val advisedChunkSize: Int            = 1
              override val advisedRateChunks: Int           = 1
              override val advisedRatePeriod: Duration      = Duration.ofSeconds(1)
              override val advisedChunksBurst: Int          = 10
            }
          )
        )

        synapseLinkReader <- ZIO.succeed(SynapseLinkReader(storageReader, sourceTableName, sourceRoot))
        synapseLinkDataProvider <- ZIO.succeed(
          SynapseLinkDataProvider(
            synapseLinkReader,
            propertyManager,
            TestSinkSettings,
            graphSettings,
            backfillSettings,
            shaper
          )
        )
        provider <- ZIO.succeed(
          SynapseLinkStreamingDataProvider(
            synapseLinkDataProvider,
            graphSettings,
            backfillSettings,
            backfillStreamContext,
            DeclaredMetrics(NullDimensionsProvider)
          )
        )
        rows <- provider.stream.runCollect()
      // expect 30 rows, since each file has 5 rows
      // total 7 files for this table (first folder doesn't have a CSV/schema for this table)
      // 1 file skipped as it is the latest one
      // plus there 1 record to be deleted
      // plus final row must be watermark row
      yield assertTrue((rows.size == 5 * (7 - 1) + 1 * (7 - 1) + 1) && rows.last.isWatermark)
    },
    test("stream correct number of changes") {
      for
        tableName         <- ZIO.succeed("target_table_stream")
        tableSinkSettings <- ZIO.succeed(TestDynamicSinkSettings(s"demo.test.$tableName"))
        propertyManager   <- ZIO.succeed(IcebergTablePropertyManager(tableSinkSettings))
        _                 <- prepareWatermark(tableSinkSettings.targetTableNameParts.Name, propertyManager)
        shaper            <- ZIO.succeed(MemoryBoundShaper(propertyManager, tableSinkSettings, TestThroughputSettings))

        synapseLinkReader <- ZIO.succeed(SynapseLinkReader(storageReader, sourceTableName, sourceRoot))
        synapseLinkDataProvider <- ZIO.succeed(
          SynapseLinkDataProvider(
            synapseLinkReader,
            propertyManager,
            tableSinkSettings,
            graphSettings,
            backfillSettings,
            shaper
          )
        )
        provider <- ZIO.succeed(
          SynapseLinkStreamingDataProvider(
            synapseLinkDataProvider,
            graphSettings,
            backfillSettings,
            changeCaptureStreamContext,
            DeclaredMetrics(NullDimensionsProvider)
          )
        )
        rows <- provider.stream.timeout(zio.Duration.fromSeconds(4)).runCount
      // expect 5 rows, since each file has 5 rows
      // total 7 files for this table (first folder doesn't have a CSV/schema for this table)
      // watermark is 3 hours back which should only capture 1 file
      // one row should be watermark
      yield assertTrue(rows == 5 + 1 + 1)
    },
    test("stream changes in the correct order") {
      for
        tableName         <- ZIO.succeed("target_table_stream_ordered")
        tableSinkSettings <- ZIO.succeed(TestDynamicSinkSettings(s"demo.test.$tableName"))
        propertyManager   <- ZIO.succeed(IcebergTablePropertyManager(tableSinkSettings))
        // prepare target table metadata
        _      <- prepareWatermark(tableSinkSettings.targetTableNameParts.Name, propertyManager)
        shaper <- ZIO.succeed(MemoryBoundShaper(propertyManager, tableSinkSettings, TestThroughputSettings))

        synapseLinkReader <- ZIO.succeed(SynapseLinkReader(storageReader, sourceTableName, sourceRoot))
        synapseLinkDataProvider <- ZIO.succeed(
          SynapseLinkDataProvider(
            synapseLinkReader,
            propertyManager,
            tableSinkSettings,
            graphSettings,
            backfillSettings,
            shaper
          )
        )
        provider <- ZIO.succeed(
          SynapseLinkStreamingDataProvider(
            synapseLinkDataProvider,
            graphSettings,
            backfillSettings,
            changeCaptureStreamContext,
            DeclaredMetrics(NullDimensionsProvider)
          )
        )
        rows <- provider.stream.filterNot(_.isWatermark).timeout(zio.Duration.fromSeconds(4)).runCollect
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
