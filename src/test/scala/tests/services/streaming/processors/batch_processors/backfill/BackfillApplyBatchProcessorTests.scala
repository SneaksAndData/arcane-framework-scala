package com.sneaksanddata.arcane.framework
package tests.services.streaming.processors.batch_processors.backfill

import models.batches.SynapseLinkBackfillOverwriteBatch
import models.schemas.ArcaneType.LongType
import models.schemas.{ArcaneSchema, Field, MergeKeyField}
import services.base.MergeServiceClient
import services.iceberg.base.{SinkEntityManager, SinkPropertyManager}
import services.iceberg.given_Conversion_ArcaneSchema_Schema
import services.streaming.processors.batch_processors.backfill.BackfillApplyBatchProcessor
import tests.shared.TablePropertiesSettings

import org.apache.iceberg.Schema
import org.easymock.EasyMock
import org.easymock.EasyMock.{replay, verify}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.easymock.EasyMockSugar
import zio.stream.ZStream
import zio.{Runtime, Unsafe, ZIO}

class BackfillApplyBatchProcessorTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
  private val runtime = Runtime.default

  private val testInput = LazyList
    .from(0)
    .takeWhile(_ < 3)
    .map { i =>
      val schema = ArcaneSchema(Seq(MergeKeyField))
      SynapseLinkBackfillOverwriteBatch(
        "intermediate-table",
        schema,
        "catalog.schema.target",
        TablePropertiesSettings,
        None
      )
    }
    .concat {
      LazyList.from(0).takeWhile(_ < 3).map { i =>
        val secondSchema = ArcaneSchema(Seq(MergeKeyField, Field("field", LongType)))
        SynapseLinkBackfillOverwriteBatch(
          s"staging_0_$i",
          secondSchema,
          "catalog.schema.target",
          TablePropertiesSettings,
          None
        )
      }
    }

  it should "run applies" in {
    // Arrange
    val mergeServiceClient  = mock[MergeServiceClient]
    val sinkPropertyManager = mock[SinkPropertyManager]
    val sinkEntityManager   = mock[SinkEntityManager]

    expecting {
      // Calling once for each batch in batch set
      mergeServiceClient.applyBatch(EasyMock.anyObject()).andReturn(ZIO.succeed(true)).times(testInput.length)
      sinkPropertyManager
        .getTableSchema(EasyMock.anyString())
        .andReturn(ZIO.succeed(implicitly[Schema](using ArcaneSchema(Seq(MergeKeyField)))))
        .times(testInput.length)
      sinkEntityManager
        .migrateSchema(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyString())
        .andReturn(ZIO.unit)
        .times(testInput.length)

    }
    replay(mergeServiceClient, sinkEntityManager, sinkPropertyManager)

    val processor = BackfillApplyBatchProcessor(mergeServiceClient, sinkEntityManager, sinkPropertyManager)

    // Act
    val stream = ZStream.fromIterable(testInput).via(processor.process).runCollect

    // Assert
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { result =>
      verify(mergeServiceClient)
      result should contain theSameElementsInOrderAs testInput
    }
  }
