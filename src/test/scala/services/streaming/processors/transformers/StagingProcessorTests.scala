package com.sneaksanddata.arcane.framework
package services.streaming.processors.transformers

import models.settings.TableFormat.PARQUET
import models.settings.*
import models.*
import services.consumers.{MergeableBatch, StagedVersionedBatch}
import services.lakehouse.base.{CatalogWriter, IcebergCatalogSettings, S3CatalogFileIO}
import services.streaming.base.{MetadataEnrichedRowStreamElement, OptimizationRequestConvertable, OrphanFilesExpirationRequestConvertable, RowGroupTransformer, SnapshotExpirationRequestConvertable, StagedBatchProcessor, ToInFlightBatch}
import utils.*

import com.sneaksanddata.arcane.framework.services.cdm.SynapseHookManager
import com.sneaksanddata.arcane.framework.services.lakehouse.{IcebergCatalogCredential, IcebergS3CatalogWriter}
import com.sneaksanddata.arcane.framework.services.merging.models.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import com.sneaksanddata.arcane.framework.services.streaming.processors.utils.TestIndexedStagedBatches
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


  def spec = suite("StagingProcessor")(
    test("write data rows grouped by schema to staging tables") {
      def toInFlightBatch(batches: Iterable[StagedVersionedBatch & MergeableBatch], index: Long, others: Any): StagedBatchProcessor#BatchType =
        new TestIndexedStagedBatches(batches, index)

      for {
              catalogWriterService <- ZIO.service[Reloadable[CatalogWriter[RESTCatalog, Table, Schema]]]
              stagingProcessor = StagingProcessor(TestStagingDataSettings,
                TestTablePropertiesSettings,
                TestTargetTableSettingsWithMaintenance,
                TestIcebergCatalogSettings,
                catalogWriterService)
              result <- ZStream.succeed(testInput).via(stagingProcessor.process(toInFlightBatch, hookManager.onBatchStaged)).run(ZSink.last)
         } yield assertTrue(result.exists(v => (v.groupedBySchema.size, v.batchIndex) == (2, 0)))
    }
  ).provide(icebergCatalogSettingsLayer, IcebergS3CatalogWriter.autoReloadable) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock



//  it should "write data rows grouped by schema to staging tables" in  {
//    // Arrange
//    val writer = IcebergS3CatalogWriter.autoReloadable
//    val tableMock = mock[Table]
//
//    expecting {
//      tableMock
//        .name()
//        .andReturn("database.namespace.name")
//        .anyTimes()
//
//      catalogWriter
//        .get
//        .write(EasyMock.anyObject[Chunk[DataRow]],EasyMock.anyString(), EasyMock.anyObject())
//        .andReturn(ZIO.succeed(tableMock))
//        .times(2)
//    }
//    replay(tableMock)
//    replay(catalogWriter)
//
//    val stagingProcessor = StagingProcessor(TestStagingDataSettings,
//      TestTablePropertiesSettings,
//      TestTargetTableSettings,
//      TestIcebergCatalogSettings,
//      catalogWriter)
//
//    def toInFlightBatch(batches: Iterable[StagedVersionedBatch & MergeableBatch], index: Long, others: Any): stagingProcessor.OutgoingElement =
//      new TestIndexedStagedBatches(batches, index)
//
//    val hookManager = SynapseHookManager()
//
//    // Act
//    val stream = ZStream.succeed(testInput).via(stagingProcessor.process(toInFlightBatch, hookManager.onBatchStaged)).run(ZSink.last)
//
//    // Assert
//    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { result =>
//      verify (catalogWriter)
//      val batch = result.get
//      (batch.groupedBySchema.size, batch.batchIndex) shouldBe(2, 0)
//    }
//  }

//  it should "allow accessing stream metadata" in {
//    // Arrange
//    val catalogWriter = mock[CatalogWriter[RESTCatalog, Table, Schema]]
//    val tableMock = mock[Table]
//
//    expecting {
//      tableMock
//        .name()
//        .andReturn("database.namespace.name")
//        .anyTimes()
//
//      catalogWriter
//        .write(EasyMock.anyObject[Chunk[DataRow]], EasyMock.anyString(), EasyMock.anyObject())
//        .andReturn(ZIO.succeed(tableMock))
//        .anyTimes()
//    }
//    replay(tableMock)
//    replay(catalogWriter)
//
//
//
//    class IndexedStagedBatchesWithMetadata(override val groupedBySchema: Iterable[StagedVersionedBatch & MergeableBatch],
//                                           override val batchIndex: Long,
//                                           val others: Chunk[String])
//      extends TestIndexedStagedBatches(groupedBySchema, batchIndex)
//
//    val stagingProcessor = StagingProcessor(TestStagingDataSettings,
//      TestTablePropertiesSettings,
//      TestTargetTableSettings,
//      TestIcebergCatalogSettings,
//      catalogWriter)
//
//    def toInFlightBatch(batches: Iterable[StagedVersionedBatch & MergeableBatch], index: Long, others: Chunk[Any]): stagingProcessor.OutgoingElement =
//      new IndexedStagedBatchesWithMetadata(batches, index, others.map(_.toString))
//
//    val hookManager = SynapseHookManager()
//
//    // Act
//    val stream = ZStream.succeed(testInput).via(stagingProcessor.process(toInFlightBatch, hookManager.onBatchStaged)).run(ZSink.last)
//
//    // Assert
//    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { result =>
//      val batch = result.get.asInstanceOf[IndexedStagedBatchesWithMetadata]
//      batch.others shouldBe Chunk("metadata", "source delete request")
//    }
//  }
//
//  it should "not not produce output on empty input" in {
//    // Arrange
//    val catalogWriter = mock[CatalogWriter[RESTCatalog, Table, Schema]]
//    val tableMock = mock[Table]
//
//    expecting {
//      tableMock
//        .name()
//        .andReturn("database.namespace.name")
//        .anyTimes()
//
//      catalogWriter
//        .write(EasyMock.anyObject[Chunk[DataRow]], EasyMock.anyString(), EasyMock.anyObject())
//        .andReturn(ZIO.succeed(tableMock))
//        .anyTimes()
//    }
//    replay(tableMock)
//    replay(catalogWriter)
//
//
//    class IndexedStagedBatchesWithMetadata(override val groupedBySchema: Iterable[StagedVersionedBatch & MergeableBatch],
//                                           override val batchIndex: Long,
//                                           val others: Chunk[String])
//      extends TestIndexedStagedBatches(groupedBySchema, batchIndex)
//
//    def toInFlightBatch(batches: Iterable[StagedVersionedBatch & MergeableBatch], index: Long, others: Chunk[Any]): StagedBatchProcessor#BatchType =
//      new IndexedStagedBatchesWithMetadata(batches, index, others.map(_.toString))
//
//    val hookManager = SynapseHookManager()
//
//    // Act
//    val stream = for {
//      catalogWriterService <- ZIO.service[Reloadable[CatalogWriter[RESTCatalog, Table, Schema]]]
//      stagingProcessor = StagingProcessor(TestStagingDataSettings,
//        TestTablePropertiesSettings,
//        TestTargetTableSettingsWithMaintenance,
//        TestIcebergCatalogSettings,
//        catalogWriterService)
//      result <- ZStream.succeed(Chunk[TestInput]()).via(stagingProcessor.process(toInFlightBatch, hookManager.onBatchStaged)).run(ZSink.last)
//    } yield result
//
//    // Assert
//    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream.orDie)).map { result =>
//      result shouldBe None
//    }
//  }
