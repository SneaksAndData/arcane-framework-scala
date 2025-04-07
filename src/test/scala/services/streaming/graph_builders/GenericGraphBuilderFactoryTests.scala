package com.sneaksanddata.arcane.framework
package services.streaming.graph_builders

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.{BackfillBehavior, BackfillSettings}
import com.sneaksanddata.arcane.framework.services.app.base.StreamLifetimeService
import com.sneaksanddata.arcane.framework.services.streaming.base.{BackfillStreamingMergeDataProvider, BackfillStreamingOverwriteDataProvider, HookManager, StreamDataProvider, StreamingGraphBuilder}
import com.sneaksanddata.arcane.framework.services.streaming.graph_builders.backfill.{GenericBackfillMergeGraphBuilder, GenericBackfillOverwriteGraphBuilder}
import com.sneaksanddata.arcane.framework.services.streaming.graph_builders.GenericStreamingGraphBuilder
import com.sneaksanddata.arcane.framework.services.streaming.processors.GenericGroupingTransformer
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.backfill.BackfillApplyBatchProcessor
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import com.sneaksanddata.arcane.framework.utils.CustomTestBackfillTableSettings
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

  private val mergeBackfillSettings = new CustomTestBackfillTableSettings(BackfillBehavior.Merge)
  private val overwriteBackfillSettings = new CustomTestBackfillTableSettings(BackfillBehavior.Overwrite)

  private val graphBuilderConditions = Table(
    ("streamContext", "backfillSettings", "expectedResult"),
    (backfillStreamContext, mergeBackfillSettings, "GenericBackfillMergeGraphBuilder"),
    (backfillStreamContext, overwriteBackfillSettings, "GenericBackfillOverwriteGraphBuilder"),
    (streamingStreamContext, overwriteBackfillSettings, "GenericStreamingGraphBuilder"),
    (streamingStreamContext, mergeBackfillSettings, "GenericStreamingGraphBuilder"),
  )

  it should "generate correct graph builder" in {
    forAll(graphBuilderConditions) { (streamContext, backfillSettings, expectedResult) =>
      // Arrange
      val service = ZIO.service[StreamingGraphBuilder]
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
          ZLayer.succeed(mock[GenericGroupingTransformer]),
          ZLayer.succeed(mock[MergeBatchProcessor]),
          ZLayer.succeed(mock[DisposeBatchProcessor]),
          ZLayer.succeed(mock[BackfillStreamingOverwriteDataProvider])
        )
      
      val getResolvedClassName = service.map(_.getClass.getName.split('.').last)

      // Act
      Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(getResolvedClassName)).map { result =>

        // Assert
        result must be(expectedResult)
      }
    }
  }
