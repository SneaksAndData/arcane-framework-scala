package com.sneaksanddata.arcane.framework
package tests.services.streaming.processors.batch_processors.backfill

import models.batches.SynapseLinkBackfillOverwriteBatch
import models.schemas.ArcaneType.LongType
import models.schemas.{ArcaneSchema, Field, MergeKeyField}
import services.base.MergeServiceClient
import services.merging.JdbcTableManager
import services.streaming.processors.batch_processors.backfill.BackfillApplyBatchProcessor
import tests.shared.TablePropertiesSettings

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
      SynapseLinkBackfillOverwriteBatch("intermediate-table", schema, "target", TablePropertiesSettings)
    }
    .concat {
      LazyList.from(0).takeWhile(_ < 3).map { i =>
        val secondSchema = ArcaneSchema(Seq(MergeKeyField, Field("field", LongType)))
        SynapseLinkBackfillOverwriteBatch(s"staging_0_$i", secondSchema, "target", TablePropertiesSettings)
      }
    }

  it should "run applies" in {
    // Arrange
    val mergeServiceClient = mock[MergeServiceClient]
    val tableManager       = mock[JdbcTableManager]

    expecting {
      // Calling once for each batch in batch set
      mergeServiceClient.applyBatch(EasyMock.anyObject()).andReturn(ZIO.succeed(true)).times(testInput.length)
      tableManager.migrateSchema(EasyMock.anyObject(), EasyMock.anyString()).andReturn(ZIO.unit).times(testInput.length)

      tableManager.optimizeTable(None).andReturn(ZIO.unit).anyTimes()
      tableManager.expireSnapshots(None).andReturn(ZIO.unit).anyTimes()
      tableManager.expireOrphanFiles(None).andReturn(ZIO.unit).anyTimes()
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
