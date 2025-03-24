package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.backfill

import models.ArcaneType.LongType
import models.{ArcaneSchema, Field, MergeKeyField}
import services.consumers.{SynapseLinkBackfillOverwriteBatch, SynapseLinkMergeBatch}
import utils.TablePropertiesSettings

import com.sneaksanddata.arcane.framework.services.base.MergeServiceClient
import com.sneaksanddata.arcane.framework.services.merging.JdbcTableManager
import com.sneaksanddata.arcane.framework.services.streaming.processors.utils.TestIndexedStagedBatches
import org.easymock.EasyMock
import org.easymock.EasyMock.{replay, verify}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.easymock.EasyMockSugar
import zio.stream.ZStream
import zio.{Runtime, Unsafe, ZIO}

class BackfillApplyBatchProcessorTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
  private val runtime = Runtime.default

  private val testInput = LazyList.from(0).takeWhile(_ < 3)
    .map { i =>
      val schema = ArcaneSchema(Seq(MergeKeyField))
      SynapseLinkBackfillOverwriteBatch("intermediate-table", schema, "target", TablePropertiesSettings)
    }
    .concat{
      LazyList.from(0).takeWhile(_ < 3).map { i =>
        val secondSchema = ArcaneSchema(Seq(MergeKeyField, Field("field", LongType)))
        SynapseLinkBackfillOverwriteBatch(s"staging_0_$i", secondSchema, "target", TablePropertiesSettings)
      }
    }

  it should "run applies" in {
    // Arrange
    val mergeServiceClient = mock[MergeServiceClient]
    val tableManager = mock[JdbcTableManager]

    expecting {
      // Calling once for each batch in batch set
      mergeServiceClient.applyBatch(EasyMock.anyObject()).andReturn(ZIO.succeed(true)).times(testInput.length)
      tableManager.migrateSchema(EasyMock.anyObject(), EasyMock.anyString()).andReturn(ZIO.unit).times(testInput.length)

    }
    replay(mergeServiceClient, tableManager)

    val processor = BackfillApplyBatchProcessor(mergeServiceClient, tableManager)

    // Act
    val stream = ZStream.fromIterable(testInput).via(processor.process).runCollect

    // Assert
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { result =>
      verify(mergeServiceClient)
      verify(tableManager)
      result should contain theSameElementsInOrderAs testInput
    }
  }
