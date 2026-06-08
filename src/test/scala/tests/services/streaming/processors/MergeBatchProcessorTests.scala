package com.sneaksanddata.arcane.framework
package tests.services.streaming.processors

import models.batches.SynapseLinkMergeBatch
import models.schemas.ArcaneType.LongType
import models.schemas.{ArcaneSchema, Field, MergeKeyField}
import services.base.MergeServiceClient
import services.iceberg.base.{SinkEntityManager, SinkPropertyManager, StagingEntityManager, StagingPropertyManager}
import services.iceberg.given_Conversion_ArcaneSchema_Schema
import services.metrics.DeclaredMetrics
import services.streaming.processors.batch_processors.streaming.MergeBatchProcessor
import tests.shared.{TablePropertiesSettings, TestSinkSettings}

import org.apache.iceberg.Schema
import org.easymock.EasyMock
import org.easymock.EasyMock.{replay, verify}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.easymock.EasyMockSugar
import zio.stream.ZStream
import zio.{Chunk, Runtime, Unsafe, ZIO}

class MergeBatchProcessorTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
  private val runtime = Runtime.default

  private val testInput = LazyList
    .from(0)
    .takeWhile(_ < 20)
    .map { i =>
      val schema = ArcaneSchema(Seq(MergeKeyField))
      val batch  = SynapseLinkMergeBatch(s"staging_$i", schema, "catalog.schema.target", TablePropertiesSettings)

      val secondSchema = ArcaneSchema(Seq(MergeKeyField, Field("field", LongType)))
      val secondBatch =
        SynapseLinkMergeBatch(s"staging_0_$i", secondSchema, "catalog.schema.target", TablePropertiesSettings)

      Seq(batch, secondBatch)
    }

  it should "run merges" in {
    // Arrange
    val mergeServiceClient = mock[MergeServiceClient]
    val declaredMetrics    = DeclaredMetrics()

    expecting {
      // Calling once for each batch in batch set
      mergeServiceClient.applyBatch(EasyMock.anyObject()).andReturn(ZIO.succeed(true)).times(40)
    }
    replay(
      mergeServiceClient
    )

    val mergeBatchProcessor =
      MergeBatchProcessor(
        mergeServiceClient,
        declaredMetrics
      )

    // Act
    val stream = ZStream.fromIterable(testInput.flatten).via(mergeBatchProcessor.process).runCollect

    // Assert
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { result =>
      verify(mergeServiceClient)
      result should contain theSameElementsInOrderAs testInput.flatten
    }
  }
