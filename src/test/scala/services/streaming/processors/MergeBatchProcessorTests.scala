package com.sneaksanddata.arcane.framework
package services.streaming.processors

import models.ArcaneType.LongType
import models.{ArcaneSchema, Field, MergeKeyField}
import services.base.{BatchOptimizationResult, MergeServiceClient}
import services.consumers.SynapseLinkMergeBatch
import services.merging.{JdbcMergeServiceClient, JdbcTableManager}
import services.streaming.processors.batch_processors.streaming.MergeBatchProcessor
import services.streaming.processors.utils.TestIndexedStagedBatches
import services.merging.models.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import services.streaming.base.{OptimizationRequestConvertable, OrphanFilesExpirationRequestConvertable, SnapshotExpirationRequestConvertable}
import services.streaming.processors.transformers.IndexedStagedBatches

import com.sneaksanddata.arcane.framework.utils.{TablePropertiesSettings, TestTargetTableSettings, TestTargetTableSettingsWithMaintenance}
import org.easymock.EasyMock
import org.easymock.EasyMock.{replay, verify}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.easymock.EasyMockSugar
import zio.stream.ZStream
import zio.{Runtime, Unsafe, ZIO}


class MergeBatchProcessorTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
  private val runtime = Runtime.default

  private val testInput = LazyList.from(0)
    .takeWhile(_ < 20)
    .map { i =>
      val schema = ArcaneSchema(Seq(MergeKeyField))
      val batch = SynapseLinkMergeBatch(s"staging_$i", schema, "target", TablePropertiesSettings)

      val secondSchema = ArcaneSchema(Seq(MergeKeyField, Field("field", LongType)))
      val secondBatch = SynapseLinkMergeBatch(s"staging_0_$i", secondSchema, "target", TablePropertiesSettings)

      TestIndexedStagedBatches(Seq(batch, secondBatch), i)
    }

  it should "run merges, optimizations and schema migrations attempts" in {
    // Arrange
    val mergeServiceClient = mock[JdbcMergeServiceClient]
    val tableManager = mock[JdbcTableManager]

    expecting {
      // Calling once for each batch in batch set
      mergeServiceClient.applyBatch(EasyMock.anyObject()).andReturn(ZIO.succeed(true)).times(40)
      mergeServiceClient.migrateSchema(EasyMock.anyObject(), EasyMock.anyString()).andReturn(ZIO.unit).times(40)

      // Calling once for each batch set
      mergeServiceClient.optimizeTable(EasyMock.anyObject()).andReturn(ZIO.succeed(BatchOptimizationResult(true))).times(20)
      mergeServiceClient.expireSnapshots(EasyMock.anyObject()).andReturn(ZIO.succeed(BatchOptimizationResult(true))).times(20)
      mergeServiceClient.expireOrphanFiles(EasyMock.anyObject()).andReturn(ZIO.succeed(BatchOptimizationResult(true))).times(20)
    }
    replay(mergeServiceClient)
    replay(tableManager)

    val mergeBatchProcessor = MergeBatchProcessor(mergeServiceClient, TestTargetTableSettingsWithMaintenance)

    // Act
    val stream = ZStream.fromIterable(testInput).via(mergeBatchProcessor.process).runCollect

    // Assert
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { result =>
      verify(mergeServiceClient)
      verify(tableManager)
      result should contain theSameElementsInOrderAs testInput
    }
  }

  it should "not run optimizations if no settings provided" in {
    // Arrange
    val mergeServiceClient = mock[JdbcMergeServiceClient]
    val tableManager = mock[JdbcTableManager]

    expecting {
      // Calling once for each batch in batch set
      mergeServiceClient.applyBatch(EasyMock.anyObject()).andReturn(ZIO.succeed(true)).times(40)
      mergeServiceClient.migrateSchema(EasyMock.anyObject(), EasyMock.anyString()).andReturn(ZIO.unit).times(40)
    }
    replay(mergeServiceClient)
    replay(tableManager)

    val mergeBatchProcessor = MergeBatchProcessor(mergeServiceClient, TestTargetTableSettings)

    // Act
    val stream = ZStream.fromIterable(testInput).via(mergeBatchProcessor.process).runCollect

    // Assert
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { result =>
      verify(mergeServiceClient)
      verify(tableManager)
      result should contain theSameElementsInOrderAs testInput
    }
  }
