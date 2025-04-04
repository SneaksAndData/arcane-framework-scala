package com.sneaksanddata.arcane.framework
package services.streaming.processors.transformers

import models.*
import services.cdm.SynapseHookManager
import services.consumers.{MergeableBatch, StagedVersionedBatch}
import services.lakehouse.base.{CatalogWriter, IcebergCatalogSettings, S3CatalogFileIO}
import services.lakehouse.{IcebergCatalogCredential, IcebergS3CatalogWriter, IdentityIcebergDataRowConverter}
import services.streaming.base.*
import services.streaming.processors.utils.TestIndexedStagedBatches
import utils.*

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.stream.{ZSink, ZStream}
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Chunk, ZIO, ZLayer}

type TestInput = DataRow|String

given MetadataEnrichedRowStreamElement[TestInput] with
  extension (element: TestInput) def isDataRow: Boolean = element.isInstanceOf[DataRow]
  extension (element: TestInput) def toDataRow: DataRow = element.asInstanceOf[DataRow]
  extension (element: DataRow) def fromDataRow: TestInput = element

object StagingProcessorTests extends ZIOSpecDefault:
  private val settings = new IcebergCatalogSettings:
    override val namespace = "test"
    override val warehouse = "polaris"
    override val catalogUri = "http://localhost:8181/api/catalog"
    override val additionalProperties: Map[String, String] = IcebergCatalogCredential.oAuth2Properties
    override val s3CatalogFileIO: S3CatalogFileIO = S3CatalogFileIO
    override val stagingLocation: Option[String] = Some("s3://tmp/polaris/test")

  private val testInput: Chunk[TestInput] = Chunk.fromIterable(List(
    List(DataCell("name", ArcaneType.StringType, "John Doe"), DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")),
    "metadata",
    List(DataCell("name", ArcaneType.StringType, "John"), DataCell("family_name", ArcaneType.StringType, "Doe"), DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")),
    "source delete request",
  ))
  private val hookManager = SynapseHookManager()
  private val icebergCatalogSettingsLayer: ZLayer[Any, Throwable, IcebergCatalogSettings] = ZLayer.succeed(settings)
  private val getProcessor = for {
    catalogWriterService <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
    stagingProcessor = StagingProcessor(TestStagingDataSettings,
      TestTablePropertiesSettings,
      TestTargetTableSettingsWithMaintenance,
      TestIcebergCatalogSettings,
      catalogWriterService)
  } yield stagingProcessor

  private def toInFlightBatch(batches: Iterable[StagedVersionedBatch & MergeableBatch], index: Long, others: Any): StagedBatchProcessor#BatchType =
    new TestIndexedStagedBatches(batches, index)


  class IndexedStagedBatchesWithMetadata(override val groupedBySchema: Iterable[StagedVersionedBatch & MergeableBatch],
                                         override val batchIndex: Long,
                                         val others: Chunk[String])
    extends TestIndexedStagedBatches(groupedBySchema, batchIndex)

  private def toInFlightBatchWithMetadata(batches: Iterable[StagedVersionedBatch & MergeableBatch], index: Long, others: Chunk[Any]): StagedBatchProcessor#BatchType =
    new IndexedStagedBatchesWithMetadata(batches, index, others.map(_.toString))    


  def spec = suite("StagingProcessor")(

    test("run with empty batch and produce no output") {
      for {
        stagingProcessor <- getProcessor
        result <- ZStream.succeed(Chunk[TestInput]()).via(stagingProcessor.process(toInFlightBatch, hookManager.onBatchStaged)).run(ZSink.last)
      } yield assertTrue(result.isEmpty)
    },

    test("write data rows grouped by schema to staging tables") {

      for {
        stagingProcessor <- getProcessor
        result <- ZStream.succeed(testInput).via(stagingProcessor.process(toInFlightBatch, hookManager.onBatchStaged)).run(ZSink.last)
      } yield assertTrue(result.exists(v => (v.groupedBySchema.size, v.batchIndex) == (2, 0)))
    },

    test("allow accessing stream metadata") {
      for {
        stagingProcessor <- getProcessor
        result <- ZStream.succeed(testInput).via(stagingProcessor.process(toInFlightBatchWithMetadata, hookManager.onBatchStaged)).run(ZSink.last)
      } yield assertTrue(result.exists(v => v.asInstanceOf[IndexedStagedBatchesWithMetadata].others == Chunk("metadata", "source delete request")))
    }

  ).provide(IdentityIcebergDataRowConverter.layer, icebergCatalogSettingsLayer, IcebergS3CatalogWriter.layer) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock
