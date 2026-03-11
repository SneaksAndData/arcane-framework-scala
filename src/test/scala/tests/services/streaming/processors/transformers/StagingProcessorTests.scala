package com.sneaksanddata.arcane.framework
package tests.services.streaming.processors.transformers

import models.*
import models.app.PluginStreamContext
import models.batches.{MergeableBatch, StagedVersionedBatch}
import models.schemas.{ArcaneType, DataCell, DataRow, MergeKeyField}
import models.settings.FieldSelectionRuleSettings
import models.settings.backfill.BackfillBehavior.Overwrite
import models.settings.backfill.{BackfillBehavior, BackfillSettings}
import models.settings.iceberg.IcebergStagingSettings
import models.settings.observability.ObservabilitySettings
import models.settings.sink.SinkSettings
import models.settings.sources.{SourceBufferingSettings, SourceSettings, StreamSourceSettings}
import models.settings.staging.StagingSettings
import models.settings.streaming.ThroughputShaperImpl.Static
import models.settings.streaming.{ChangeCaptureSettings, StreamModeSettings, ThroughputSettings, ThroughputShaperImpl}
import services.iceberg.base.CatalogWriter
import services.iceberg.{IcebergEntityManager, IcebergS3CatalogWriter}
import services.metrics.DeclaredMetrics
import services.streaming.base.*
import services.streaming.processors.transformers.StagingProcessor
import services.synapse.SynapseHookManager
import tests.services.streaming.processors.utils.TestIndexedStagedBatches
import tests.shared.*
import tests.shared.IcebergCatalogInfo.*

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.stream.{ZSink, ZStream}
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Chunk, Scope, ZIO, ZLayer}

import java.time.{Duration, OffsetDateTime, ZoneOffset}

type TestInput = DataRow

given MetadataEnrichedRowStreamElement[TestInput] with
  extension (element: TestInput) def isDataRow: Boolean   = element.isInstanceOf[DataRow]
  extension (element: TestInput) def toDataRow: DataRow   = element
  extension (element: DataRow) def fromDataRow: TestInput = element

object StagingProcessorTests extends ZIOSpecDefault:
  private val testInput: Chunk[TestInput] = Chunk.fromIterable(
    List(
      List(
        DataCell("name", ArcaneType.StringType, "John Doe"),
        DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")
      ),
      List(
        DataCell("name", ArcaneType.StringType, "John"),
        DataCell("family_name", ArcaneType.StringType, "Doe"),
        DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")
      )
    )
  )
  private val hookManager = SynapseHookManager()
  private val icebergCatalogSettingsLayer: ZLayer[Any, Throwable, IcebergStagingSettings] =
    ZLayer.succeed(defaultIcebergStagingSettings)
  private val getProcessor = for {
    catalogWriterService <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
    stagingProcessor = StagingProcessor(
      TestStagingTableSettings,
      TestSinkSettingsWithMaintenance.targetTableFullName,
      TestIcebergStagingSettings,
      catalogWriterService,
      DeclaredMetrics(NullDimensionsProvider)
    )
  } yield stagingProcessor

  private def toInFlightBatch(
      batches: Iterable[StagedVersionedBatch & MergeableBatch],
      index: Long,
      others: Any
  ): StagedBatchProcessor#BatchType =
    new TestIndexedStagedBatches(batches, index)

  class IndexedStagedBatchesWithMetadata(
      override val groupedBySchema: Iterable[StagedVersionedBatch & MergeableBatch],
      override val batchIndex: Long,
      val others: Chunk[String]
  ) extends TestIndexedStagedBatches(groupedBySchema, batchIndex)

  private def toInFlightBatchWithMetadata(
      batches: Iterable[StagedVersionedBatch & MergeableBatch],
      index: Long,
      others: Chunk[Any]
  ): StagedBatchProcessor#BatchType =
    new IndexedStagedBatchesWithMetadata(batches, index, others.map(_.toString))

  private val mockPluginContextLayer = ZLayer.succeed(new PluginStreamContext {
    override val streamMode: StreamModeSettings = new StreamModeSettings {

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
        override val changeCaptureJitterVariance: Double = 0.01
        override val changeCaptureJitterSeed: Long       = 0
      }
    }
    override val sink: SinkSettings = TestSinkSettings
    override val source: StreamSourceSettings = new StreamSourceSettings {
      override type SourceSettingsType = SourceSettings
      override val configuration: SourceSettingsType              = new SourceSettings {}
      override val buffering: SourceBufferingSettings             = TestSourceBufferingSettings
      override val fieldSelectionRule: FieldSelectionRuleSettings = TestFieldSelectionRuleSettings
    }
    override val staging: StagingSettings             = TestStagingSettings()
    override val observability: ObservabilitySettings = TestObservabilitySettings
    override val throughput: ThroughputSettings = new ThroughputSettings {
      override val shaperImpl: ThroughputShaperImpl = Static
      override val advisedChunkSize: Int            = 1
      override val advisedRateChunks: Int           = 1
      override val advisedRatePeriod: Duration      = Duration.ofSeconds(1)
      override val advisedChunksBurst: Int          = 10
    }

    override def merge(other: Option[PluginStreamContext]): PluginStreamContext = ???
  })

  def spec: Spec[TestEnvironment & Scope, Throwable] = suite("StagingProcessor")(
    test("run with empty batch and produce no output") {
      for {
        stagingProcessor <- getProcessor
        result <- ZStream
          .fromIterable(Chunk[TestInput]())
          .via(stagingProcessor.process(toInFlightBatch, hookManager.onBatchStaged))
          .run(ZSink.last)
      } yield assertTrue(result.isEmpty)
    },
    test("write data rows grouped by schema to staging tables") {

      for {
        stagingProcessor <- getProcessor
        result <- ZStream
          .fromIterable(testInput)
          .via(stagingProcessor.process(toInFlightBatch, hookManager.onBatchStaged))
          .run(ZSink.last)
      } yield assertTrue(result.exists(v => (v.groupedBySchema.size, v.batchIndex) == (2, 0)))
    }
  ).provide(
    IcebergEntityManager.stagingLayer,
    IcebergS3CatalogWriter.layer,
    mockPluginContextLayer
  ) @@ timeout(
    zio.Duration.fromSeconds(60)
  ) @@ TestAspect.withLiveClock
