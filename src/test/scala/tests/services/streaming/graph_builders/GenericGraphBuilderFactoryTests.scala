package com.sneaksanddata.arcane.framework
package tests.services.streaming.graph_builders

import models.app.StreamContext
import models.settings.BackfillBehavior
import services.app.base.StreamLifetimeService
import services.streaming.base.*
import services.streaming.graph_builders.GenericGraphBuilderFactory
import services.streaming.processors.batch_processors.backfill.{
  BackfillApplyBatchProcessor,
  BackfillOverwriteWatermarkProcessor
}
import services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  WatermarkProcessor
}
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import tests.shared.{CustomTestBackfillTableSettings, TestSourceBufferingSettings}

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table
import org.scalatestplus.easymock.EasyMockSugar
import zio.{Runtime, Unsafe, ZIO, ZLayer}

class GenericGraphBuilderFactoryTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
  private val runtime = Runtime.default

  private val backfillStreamContext = new StreamContext:
    override val IsBackfilling: Boolean = true

  private val streamingStreamContext = new StreamContext:
    override val IsBackfilling: Boolean = false

  private val mergeBackfillSettings     = new CustomTestBackfillTableSettings(BackfillBehavior.Merge)
  private val overwriteBackfillSettings = new CustomTestBackfillTableSettings(BackfillBehavior.Overwrite)

  private val graphBuilderConditions = Table(
    ("streamContext", "backfillSettings", "expectedResult"),
    (backfillStreamContext, mergeBackfillSettings, "GenericBackfillMergeGraphBuilder"),
    (backfillStreamContext, overwriteBackfillSettings, "GenericBackfillOverwriteGraphBuilder"),
    (streamingStreamContext, overwriteBackfillSettings, "GenericStreamingGraphBuilder"),
    (streamingStreamContext, mergeBackfillSettings, "GenericStreamingGraphBuilder")
  )

  it should "generate correct graph builder" in {
    forAll(graphBuilderConditions) { (streamContext, backfillSettings, expectedResult) =>
      // Arrange
      val service = ZIO
        .service[StreamingGraphBuilder]
        .provide(
          GenericGraphBuilderFactory.composedLayer,
          ZLayer.succeed(backfillSettings),
          ZLayer.succeed(streamContext),
          ZLayer.succeed(mock[StreamDataProvider]),
          ZLayer.succeed(mock[BackfillStreamingMergeDataProvider]),
          ZLayer.succeed(mock[BackfillApplyBatchProcessor]),
          ZLayer.succeed(mock[StagingProcessor]),
          ZLayer.succeed(mock[FieldFilteringTransformer]),
          ZLayer.succeed(mock[StreamLifetimeService]),
          ZLayer.succeed(mock[MergeBatchProcessor]),
          ZLayer.succeed(mock[DisposeBatchProcessor]),
          ZLayer.succeed(mock[BackfillStreamingOverwriteDataProvider]),
          ZLayer.succeed(TestSourceBufferingSettings),
          ZLayer.succeed(mock[WatermarkProcessor]),
          ZLayer.succeed(mock[BackfillOverwriteWatermarkProcessor])
        )

      val getResolvedClassName = service.map(_.getClass.getName.split('.').last)

      // Act
      Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(getResolvedClassName)).map { result =>
        // Assert
        result must be(expectedResult)
      }
    }
  }
