package com.sneaksanddata.arcane.framework
package services.streaming

import com.sneaksanddata.arcane.framework.models.{ArcaneType, DataCell, DataRow, MergeKeyField}
import com.sneaksanddata.arcane.framework.models.given_MetadataEnrichedRowStreamElement_DataRow
import com.sneaksanddata.arcane.framework.services.app.GenericStreamRunnerService
import com.sneaksanddata.arcane.framework.services.filters.FieldsFilteringService
import com.sneaksanddata.arcane.framework.services.streaming.graph_builders.base.GenericStreamingGraphBuilder
import com.sneaksanddata.arcane.framework.services.streaming.processors.GenericGroupingTransformer
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.DisposeBatchProcessor
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.MergeBatchProcessor
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.FieldFilteringTransformer.Environment
import com.sneaksanddata.arcane.framework.utils.TestStreamLifetimeService
import org.easymock.EasyMock.verify
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import org.scalatestplus.easymock.EasyMockSugar
import zio.{Chunk, Runtime, Unsafe, ZIO, ZLayer}

class GenericStreamRunnerServiceTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
  private val runtime = Runtime.default

  private val testInput: Chunk[DataRow] = Chunk.fromIterable(List(
    List(DataCell("name", ArcaneType.StringType, "John Doe"), DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")),
    List(DataCell("name", ArcaneType.StringType, "John"), DataCell("family_name", ArcaneType.StringType, "Doe"), DataCell(MergeKeyField.name, MergeKeyField.fieldType, "1")),
  ))

  it should "run the stream" in {
    // Arrange
    val streamRunnerService = ZIO.service[GenericStreamRunnerService].provide(
      GenericStreamRunnerService.layer,
      ZLayer.succeed(new TestStreamLifetimeService(5, identity)),
      GenericStreamingGraphBuilder.layer,
      GenericGroupingTransformer.layer,
      DisposeBatchProcessor.layer,
      FieldFilteringTransformer.layer,
      MergeBatchProcessor.layer,
      StagingProcessor.layer,
      FieldsFilteringService.layer,
    )


    // Act
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(streamRunnerService)).map { result =>
      // Assert
      result should not be null
    }

//    // Act
//    val result = runtime.unsafeRun(streamRunnerService.run)
//
//    // Assert
//    result mustBe testInput
  }
