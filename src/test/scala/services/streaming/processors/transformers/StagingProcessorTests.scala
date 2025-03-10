package com.sneaksanddata.arcane.framework
package services.streaming.processors.transformers

import models.settings.TableFormat.PARQUET
import models.settings.*
import models.*
import services.consumers.{MergeableBatch, StagedVersionedBatch}
import services.lakehouse.base.{CatalogWriter, IcebergCatalogSettings, S3CatalogFileIO}
import services.streaming.base.{MetadataEnrichedRowStreamElement, OptimizationRequestConvertable, OrphanFilesExpirationRequestConvertable, RowGroupTransformer, SnapshotExpirationRequestConvertable, ToInFlightBatch}
import utils.*

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
import zio.{Chunk, Runtime, Unsafe, ZIO}

import scala.concurrent.Future

type TestInput = DataRow|String

given MetadataEnrichedRowStreamElement[TestInput] with
  extension (element: TestInput) def isDataRow: Boolean = element.isInstanceOf[DataRow]
  extension (element: TestInput) def toDataRow: DataRow = element.asInstanceOf[DataRow]
  extension (element: DataRow) def fromDataRow: TestInput = element

class StagingProcessorTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
  private val runtime = Runtime.default

  private val testInput: Chunk[TestInput] = Chunk.fromIterable(List(
    List(DataCell("name", ArcaneType.StringType, "John Doe"), DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")),
    "metadata",
    List(DataCell("name", ArcaneType.StringType, "John"), DataCell("family_name", ArcaneType.StringType, "Doe"), DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")),
    "source delete request",
  ))

  it should "write data rows grouped by schema to staging tables" in  {
    // Arrange
    val catalogWriter = mock[CatalogWriter[RESTCatalog, Table, Schema]]
    val tableMock = mock[Table]

    expecting{
      tableMock
        .name()
        .andReturn("database.namespace.name")
        .anyTimes()

      catalogWriter
        .write(EasyMock.anyObject[Chunk[DataRow]],EasyMock.anyString(), EasyMock.anyObject())
        .andReturn(ZIO.succeed(tableMock))
        .times(2)
    }
    replay(tableMock)
    replay(catalogWriter)

    val stagingProcessor = StagingProcessor(TestStagingDataSettings,
      TestTablePropertiesSettings,
      TestTargetTableSettings,
      TestIcebergCatalogSettings,
      catalogWriter)

    def toInFlightBatch(batches: Iterable[StagedVersionedBatch & MergeableBatch], index: Long, others: Any): stagingProcessor.OutgoingElement =
      new TestIndexedStagedBatches(batches, index)

    // Act
    val stream = ZStream.succeed(testInput).via(stagingProcessor.process(toInFlightBatch)).run(ZSink.last)

    // Assert
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { result =>
      verify (catalogWriter)
      val batch = result.get
      (batch.groupedBySchema.size, batch.batchIndex) shouldBe(2, 0)
    }
  }

  it should "allow accessing stream metadata" in {
    // Arrange
    val catalogWriter = mock[CatalogWriter[RESTCatalog, Table, Schema]]
    val tableMock = mock[Table]

    expecting {
      tableMock
        .name()
        .andReturn("database.namespace.name")
        .anyTimes()

      catalogWriter
        .write(EasyMock.anyObject[Chunk[DataRow]], EasyMock.anyString(), EasyMock.anyObject())
        .andReturn(ZIO.succeed(tableMock))
        .anyTimes()
    }
    replay(tableMock)
    replay(catalogWriter)



    class IndexedStagedBatchesWithMetadata(override val groupedBySchema: Iterable[StagedVersionedBatch & MergeableBatch],
                                           override val batchIndex: Long,
                                           val others: Chunk[String])
      extends TestIndexedStagedBatches(groupedBySchema, batchIndex)
      
    val stagingProcessor = StagingProcessor(TestStagingDataSettings,
      TestTablePropertiesSettings,
      TestTargetTableSettings,
      TestIcebergCatalogSettings,
      catalogWriter)
      
    def toInFlightBatch(batches: Iterable[StagedVersionedBatch & MergeableBatch], index: Long, others: Chunk[Any]): stagingProcessor.OutgoingElement =
      new IndexedStagedBatchesWithMetadata(batches, index, others.map(_.toString))

    // Act
    val stream = ZStream.succeed(testInput).via(stagingProcessor.process(toInFlightBatch)).run(ZSink.last)

    // Assert
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { result =>
      val batch = result.get.asInstanceOf[IndexedStagedBatchesWithMetadata]
      batch.others shouldBe Chunk("metadata", "source delete request")
    }
  }

  it should "not not produce output on empty input" in {
    // Arrange
    val catalogWriter = mock[CatalogWriter[RESTCatalog, Table, Schema]]
    val tableMock = mock[Table]

    expecting {
      tableMock
        .name()
        .andReturn("database.namespace.name")
        .anyTimes()

      catalogWriter
        .write(EasyMock.anyObject[Chunk[DataRow]], EasyMock.anyString(), EasyMock.anyObject())
        .andReturn(ZIO.succeed(tableMock))
        .anyTimes()
    }
    replay(tableMock)
    replay(catalogWriter)


    class IndexedStagedBatchesWithMetadata(override val groupedBySchema: Iterable[StagedVersionedBatch & MergeableBatch],
                                           override val batchIndex: Long,
                                           val others: Chunk[String])
      extends TestIndexedStagedBatches(groupedBySchema, batchIndex)

    val stagingProcessor = StagingProcessor(TestStagingDataSettings,
      TestTablePropertiesSettings,
      TestTargetTableSettingsWithMaintenance,
      TestIcebergCatalogSettings,
      catalogWriter)

    def toInFlightBatch(batches: Iterable[StagedVersionedBatch & MergeableBatch], index: Long, others: Chunk[Any]): stagingProcessor.OutgoingElement =
      new IndexedStagedBatchesWithMetadata(batches, index, others.map(_.toString))

    // Act
    val stream = ZStream.succeed(Chunk[TestInput]()).via(stagingProcessor.process(toInFlightBatch)).run(ZSink.last)

    // Assert
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { result =>
      result shouldBe None
    }
  }
