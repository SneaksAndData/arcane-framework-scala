package com.sneaksanddata.arcane.framework
package services.streaming.data_providers.backfill

import logging.ZIOLogAnnotations.zlog
import models.settings.BackfillSettings
import services.app.base.StreamLifetimeService
import services.streaming.base.{BackfillOverwriteBatchFactory, BackfillStreamingMergeDataProvider, BackfillSubStream, HookManager}

import zio.stream.ZPipeline
import zio.{Task, ZIO, ZLayer}

/**
 * Provides the backfill data stream for the streaming process. This data provider is used when backfill started with
 * the `Merge` behavior.
 * @param streamingGraphBuilder The streaming graph builder.
 *                              This is the main component that provides the stream of data.
 * @param lifetimeService The stream lifetime service.
 *                        This service is used to manage the lifetime of the stream.
 * @param hookManager The hook manager.
 *                    This manager is used to manage the hooks that are used in the streaming process.
 */
class GenericBackfillStreamingMergeDataProvider(streamingGraphBuilder: BackfillSubStream,
                                                lifetimeService: StreamLifetimeService,
                                                hookManager: HookManager)
  extends BackfillStreamingMergeDataProvider:

  /**
   * @inheritdoc
   */
  def requestBackfill: Task[Unit] =
    for
      _ <- zlog("Starting backfill process")
      _ <- streamingGraphBuilder.produce(hookManager).via(streamLifetimeGuard).runDrain
      _ <- zlog("Backfill process completed")
    yield ()

  private def streamLifetimeGuard = ZPipeline.takeUntil(_ => lifetimeService.cancelled)


/**
 * The companion object for the GenericBackfillStreamingMergeDataProvider class.
 */
object GenericBackfillStreamingMergeDataProvider:

  /**
   * The environment required for the GenericBackfillStreamingMergeDataProvider.
   */
  type Environment = BackfillSubStream
    & BackfillSettings
    & StreamLifetimeService
    & BackfillOverwriteBatchFactory
    & HookManager

  /**
   * Creates a new GenericBackfillStreamingMergeDataProvider.
   * @param streamingGraphBuilder The streaming graph builder.
   * @param lifetimeService The stream lifetime service.
   * @param hookManager The hook manager.
   * @return The GenericBackfillStreamingMergeDataProvider instance.
   */
  def apply(streamingGraphBuilder: BackfillSubStream,
            lifetimeService: StreamLifetimeService,
            hookManager: HookManager): GenericBackfillStreamingMergeDataProvider =
    new GenericBackfillStreamingMergeDataProvider(streamingGraphBuilder, lifetimeService, hookManager)


  /**
   * The ZLayer for the GenericBackfillStreamingMergeDataProvider.
   */
  val layer: ZLayer[Environment, Nothing, GenericBackfillStreamingMergeDataProvider] =
    ZLayer {
      for
        streamingGraphBuilder <- ZIO.service[BackfillSubStream]
        lifetimeService <- ZIO.service[StreamLifetimeService]
        hookManager <- ZIO.service[HookManager]
      yield GenericBackfillStreamingMergeDataProvider(streamingGraphBuilder, lifetimeService, hookManager)
    }
