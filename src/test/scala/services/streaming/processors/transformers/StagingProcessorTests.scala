package com.sneaksanddata.arcane.framework
package services.streaming.processors.transformers

import models.settings.TableFormat.PARQUET
import models.settings.*
import models.*
import services.consumers.StagedVersionedBatch
import services.lakehouse.base.IcebergCatalogSettings
import services.lakehouse.{CatalogWriter, S3CatalogFileIO}
import services.streaming.base.{MetadataEnrichedRowStreamElement, RowGroupTransformer, ToInFlightBatch}
import utils.*

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import org.easymock.EasyMock
import org.easymock.EasyMock.{createMock, expect, replay}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.easymock.EasyMockSugar
import zio.stream.ZStream
import zio.{Chunk, Runtime, Unsafe}

import scala.concurrent.Future

given MetadataEnrichedRowStreamElement[DataRow] with
  extension (element: DataRow) def isDataRow: Boolean = true
  extension (element: DataRow) def toDataRow: DataRow = element
  extension (element: DataRow) def fromDataRow: DataRow = element

class StagingProcessorTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
  private val runtime = Runtime.default

  private val testInput: Chunk[DataRow] = Chunk.fromIterable(List(
    List(DataCell("name", ArcaneType.StringType, "John Doe"), DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")),
    List(DataCell("name", ArcaneType.StringType, "John"), DataCell("family_name", ArcaneType.StringType, "Doe"), DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1"))
  ))

  it should "group data rows by schema" in  {
    val catalogWriter = mock[CatalogWriter[RESTCatalog, Table, Schema]]
    val tableMock = mock[Table]

    expecting{
      tableMock
        .name()
        .andReturn("database.namespace.name")
        .anyTimes()

      catalogWriter
        .write(EasyMock.anyObject[Chunk[DataRow]],EasyMock.anyString(), EasyMock.anyObject())
        .andReturn(Future.successful(tableMock))
        .anyTimes()
    }
    replay(tableMock)
    replay(catalogWriter)

    val stagingProcessor = StagingProcessor(TestStagingDataSettings,
      TestTablePropertiesSettings,
      TestTargetTableSettings,
      TestIcebergCatalogSettings,
      TestArchiveTableSettings,
      catalogWriter)

    def toInFlightBatch(batches: Iterable[StagedVersionedBatch], index: Long, others: Any): stagingProcessor.OutgoingElement =
      new IndexedStagedBatches(batches, index){};

    val stream = ZStream.succeed(testInput).via(stagingProcessor.process(toInFlightBatch)).runCollect
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { result =>
      result should have size 1
    }
  }

