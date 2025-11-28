package com.sneaksanddata.arcane.framework
package tests.services.streaming.processors.transformers

import models.*
import models.batches.{MergeableBatch, StagedVersionedBatch}
import models.schemas.{ArcaneType, DataCell, DataRow, MergeKeyField}
import models.settings.IcebergCatalogSettings
import services.base.DimensionsProvider
import services.iceberg.IcebergS3CatalogWriter
import services.iceberg.base.CatalogWriter
import services.metrics.DeclaredMetrics
import services.streaming.base.*
import services.streaming.processors.transformers.StagingProcessor
import services.synapse.SynapseHookManager
import tests.services.streaming.processors.utils.TestIndexedStagedBatches
import tests.shared.IcebergCatalogInfo.*
import tests.shared.{
  TestIcebergCatalogSettings,
  TestStagingDataSettings,
  TestTablePropertiesSettings,
  TestTargetTableSettingsWithMaintenance
}

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.stream.{ZSink, ZStream}
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Chunk, Scope, ZIO, ZLayer}

import scala.collection.immutable.SortedMap

type TestInput = DataRow

given MetadataEnrichedRowStreamElement[TestInput] with
  extension (element: TestInput) def isDataRow: Boolean   = element.isInstanceOf[DataRow]
  extension (element: TestInput) def toDataRow: DataRow   = element
  extension (element: DataRow) def fromDataRow: TestInput = element

object NullDimensionsProvider extends DimensionsProvider:
  override def getDimensions: SortedMap[String, String] = SortedMap()

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
  private val icebergCatalogSettingsLayer: ZLayer[Any, Throwable, IcebergCatalogSettings] =
    ZLayer.succeed(defaultSettings)
  private val getProcessor = for {
    catalogWriterService <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
    stagingProcessor = StagingProcessor(
      TestStagingDataSettings,
      TestTablePropertiesSettings,
      TestTargetTableSettingsWithMaintenance,
      TestIcebergCatalogSettings,
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

  def spec: Spec[TestEnvironment & Scope, Throwable] = suite("StagingProcessor")(
    test("run with empty batch and produce no output") {
      for {
        stagingProcessor <- getProcessor
        result <- ZStream
          .succeed(Chunk[TestInput]())
          .via(stagingProcessor.process(toInFlightBatch, hookManager.onBatchStaged))
          .run(ZSink.last)
      } yield assertTrue(result.isEmpty)
    },
    test("write data rows grouped by schema to staging tables") {

      for {
        stagingProcessor <- getProcessor
        result <- ZStream
          .succeed(testInput)
          .via(stagingProcessor.process(toInFlightBatch, hookManager.onBatchStaged))
          .run(ZSink.last)
      } yield assertTrue(result.exists(v => (v.groupedBySchema.size, v.batchIndex) == (2, 0)))
    }
  ).provide(icebergCatalogSettingsLayer, IcebergS3CatalogWriter.layer) @@ timeout(
    zio.Duration.fromSeconds(60)
  ) @@ TestAspect.withLiveClock
