package com.sneaksanddata.arcane.framework
package services.streaming.processors.transformers

import models.settings.TableFormat.PARQUET
import models.settings.*
import models.*
import services.consumers.{MergeableBatch, StagedVersionedBatch}
import services.lakehouse.base.{CatalogWriter, IcebergCatalogSettings, S3CatalogFileIO}
import services.streaming.base.{MetadataEnrichedRowStreamElement, OptimizationRequestConvertable, OrphanFilesExpirationRequestConvertable, RowGroupTransformer, SnapshotExpirationRequestConvertable, StagedBatchProcessor, ToInFlightBatch}
import utils.*
import services.lakehouse.{IcebergCatalogCredential, IcebergS3CatalogWriter}
import services.merging.models.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import services.streaming.processors.utils.TestIndexedStagedBatches
import services.synapse.SynapseHookManager
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import org.easymock.EasyMock
import org.easymock.EasyMock.{createMock, expect, replay, verify}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.easymock.EasyMockSugar
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Reloadable, Runtime, Unsafe, ZIO, ZLayer}
import zio.test.*
import zio.test.TestAspect.timeout

import scala.concurrent.Future

type TestInput = DataRow|String

given MetadataEnrichedRowStreamElement[TestInput] with
  extension (element: TestInput) def isDataRow: Boolean = element.isInstanceOf[DataRow]
  extension (element: TestInput) def toDataRow: DataRow = element.asInstanceOf[DataRow]
  extension (element: DataRow) def fromDataRow: TestInput = element

object StagingProcessorTests extends ZIOSpecDefault:
  private val settings = new IcebergCatalogSettings:
    override val namespace = "test"
    override val warehouse = "demo"
    override val catalogUri = "http://localhost:20001/catalog"
    override val additionalProperties: Map[String, String] = IcebergCatalogCredential.oAuth2Properties
    override val s3CatalogFileIO: S3CatalogFileIO = S3CatalogFileIO
    override val stagingLocation: Option[String] = None

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

  ).provide(icebergCatalogSettingsLayer, IcebergS3CatalogWriter.layer) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock
