package com.sneaksanddata.arcane.framework
package tests.services.streaming.processors.transformers

import models.app.PluginStreamContext
import models.schemas.*
import models.schemas.ArcaneType.StringType
import models.settings.backfill.BackfillBehavior.Overwrite
import models.settings.backfill.{BackfillBehavior, BackfillSettings}
import models.settings.observability.ObservabilitySettings
import models.settings.sink.SinkSettings
import models.settings.sources.{SourceBufferingSettings, SourceSettings, StreamSourceSettings}
import models.settings.staging.StagingSettings
import models.settings.streaming.*
import models.settings.{FieldSelectionRuleSettings, FlowRate}
import services.iceberg.base.CatalogWriter
import services.iceberg.{IcebergEntityManager, IcebergS3CatalogWriter}
import services.metrics.DeclaredMetrics
import services.streaming.base.*
import services.streaming.processors.transformers.StagingProcessor
import tests.shared.*

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
  private val testSchema: ArcaneSchema = ArcaneSchema(
    Seq(
      IndexedMergeKeyField(1),
      IndexedField("name", StringType, 2)
    )
  )
  private val testInput: Chunk[TestInput] = Chunk.fromIterable(
    List(
      List(
        DataCell("name", ArcaneType.StringType, "John Doe"),
        DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")
      ),
      List(
        DataCell("name", ArcaneType.StringType, "John"),
        DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")
      )
    )
  )
  private val getProcessor = for {
    catalogWriterService <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
    stagingProcessor = StagingProcessor(
      TestStagingTableSettings,
      TestSinkSettings.targetTableFullName,
      TestIcebergStagingSettings,
      catalogWriterService,
      new TestStagedBatchFactory(),
      DeclaredMetrics()
    )
  } yield stagingProcessor

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
      override val shaperImpl: ThroughputShaperImpl = StaticImpl(Static())
      override val advisedChunkSize: Int            = 1
      override val advisedRate: FlowRate            = FlowRate(elements = 1, interval = Duration.ofSeconds(10))
      override val advisedBurst: Int                = 10
    }

    override def merge(other: Option[PluginStreamContext]): PluginStreamContext = ???
  })

  def spec: Spec[TestEnvironment & Scope, Throwable] = suite("StagingProcessor")(
    test("run with empty batch and produce no output") {
      for {
        stagingProcessor <- getProcessor
        result <- ZStream
          .fromIterable(Chunk[TestInput]())
          .via(stagingProcessor.process(testSchema))
          .run(ZSink.last)
      } yield assertTrue(result.isEmpty)
    },
    test("write data rows grouped by schema to staging tables") {

      for {
        stagingProcessor <- getProcessor
        result <- ZStream
          .fromIterable(testInput)
          .via(stagingProcessor.process(testSchema))
          .run(ZSink.last)
      } yield assertTrue(result.size == 1 && result.head.name.startsWith("staging_table__"))
    }
  ).provide(
    IcebergEntityManager.stagingLayer,
    IcebergS3CatalogWriter.layer,
    mockPluginContextLayer
  ) @@ timeout(
    zio.Duration.fromSeconds(60)
  ) @@ TestAspect.withLiveClock
