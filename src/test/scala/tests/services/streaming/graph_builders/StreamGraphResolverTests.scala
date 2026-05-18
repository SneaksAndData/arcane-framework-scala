//package com.sneaksanddata.arcane.framework
//package tests.services.streaming.graph_builders
//
//import models.settings.backfill.BackfillBehavior
//import services.app.base.StreamLifetimeService
//import services.streaming.base.*
//import services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor, SchemaMigrationProcessor, WatermarkProcessor}
//import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
//import tests.shared.{CustomTestBackfillTableSettings, TestPluginBackfillMergeStreamContext, TestPluginBackfillOverwriteStreamContext, TestPluginStreamContext}
//import services.streaming.batching.StagedBatchFactory
//import services.streaming.processors.batch_processors.maintenance.TargetMaintenanceProcessor
//import com.sneaksanddata.arcane.framework.services.app.StreamGraphResolver
//import com.sneaksanddata.arcane.framework.services.backfill.processors.{ShardCombineProcessor, BackfillCompletionProcessor}
//
//import com.sneaksanddata.arcane.framework.services.backfill.{BackfillStreamDataProvider, BackfillStreamingOverwriteDataProvider}
//import org.scalatest.flatspec.AsyncFlatSpec
//import org.scalatest.matchers.must.Matchers
//import org.scalatest.prop.TableDrivenPropertyChecks.forAll
//import org.scalatest.prop.Tables.Table
//import org.scalatestplus.easymock.EasyMockSugar
//import zio.{Runtime, Unsafe, ZIO, ZLayer}
//
//class StreamGraphResolverTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
//  private val runtime = Runtime.default
//
//  private val mergeBackfillSettings     = new CustomTestBackfillTableSettings(BackfillBehavior.Merge)
//  private val overwriteBackfillSettings = new CustomTestBackfillTableSettings(BackfillBehavior.Overwrite)
//
//  private val graphBuilderConditions = Table(
//    ("streamContext", "backfillSettings", "expectedResult"),
//    (TestPluginBackfillMergeStreamContext, mergeBackfillSettings, "GenericBackfillMergeGraphBuilder"),
//    (TestPluginBackfillOverwriteStreamContext, overwriteBackfillSettings, "GenericBackfillOverwriteGraphBuilder"),
//    (TestPluginStreamContext, overwriteBackfillSettings, "GenericStreamingGraphBuilder"),
//    (TestPluginStreamContext, mergeBackfillSettings, "GenericStreamingGraphBuilder")
//  )
//
//  it should "generate correct graph builder" in {
//    forAll(graphBuilderConditions) { (streamContext, backfillSettings, expectedResult) =>
//      // Arrange
//      val service = ZIO
//        .service[StreamingGraphBuilder]
//        .provide(
//          StreamGraphResolver.composedLayer,
//          ZLayer.succeed(streamContext),
//          ZLayer.succeed(mock[StreamDataProvider]),
//          ZLayer.succeed(mock[BackfillStreamDataProvider]),
//          ZLayer.succeed(mock[ShardCombineProcessor]),
//          ZLayer.succeed(mock[StagingProcessor]),
//          ZLayer.succeed(mock[FieldFilteringTransformer]),
//          ZLayer.succeed(mock[StreamLifetimeService]),
//          ZLayer.succeed(mock[MergeBatchProcessor]),
//          ZLayer.succeed(mock[DisposeBatchProcessor]),
//          ZLayer.succeed(mock[BackfillStreamingOverwriteDataProvider]),
//          ZLayer.succeed(mock[WatermarkProcessor]),
//          ZLayer.succeed(mock[BackfillCompletionProcessor]),
//          ZLayer.succeed(mock[SchemaMigrationProcessor]),
//          ZLayer.succeed(mock[TargetMaintenanceProcessor])
//        )
//
//      val getResolvedClassName = service.map(_.getClass.getName.split('.').last)
//
//      // Act
//      Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(getResolvedClassName)).map { result =>
//        // Assert
//        result must be(expectedResult)
//      }
//    }
//  }
