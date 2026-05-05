package com.sneaksanddata.arcane.framework
package services.app

import logging.ZIOLogAnnotations.zlog
import models.app.PluginStreamContext
import models.settings.staging.StagingTableSettings
import services.app.base.{StreamLifetimeService, StreamRunnerService}
import services.bootstrap.base.StreamBootstrapper
import services.metrics.base.MetricTagProvider
import services.streaming.base.{HookManager, StreamingGraphBuilder}

import zio.stream.{ZPipeline, ZSink}
import zio.{Tag, ZIO, ZIOAspect, ZLayer}

/** A service that can be used to run a stream.
  *
  * @param builder
  *   The stream graph builder.
  * @param lifetimeService
  *   The stream lifetime service.
  */
class GenericStreamRunnerService(
    builder: StreamingGraphBuilder,
    lifetimeService: StreamLifetimeService,
    stagingDataSettings: StagingTableSettings,
    hookManager: HookManager,
    bootstrapper: StreamBootstrapper,
    tagProvider: MetricTagProvider
) extends StreamRunnerService:

  /** Runs the stream.
    *
    * @return
    *   A ZIO effect that represents the stream.
    */
  def run: ZIO[Any, Throwable, Unit] =
    lifetimeService.start()
    ZIO
      .attempt(tagProvider.getTags)
      .flatMap(tags =>
        (for
          _ <- zlog("Starting the stream runner")
          _ <- bootstrapper.cleanupStagingTables(
            stagingDataSettings.stagingTablePrefix
          )

          _ <- bootstrapper.createTargetTable
          _ <- bootstrapper.createBackFillTable
          _ <- builder.produce(hookManager).via(streamLifetimeGuard).run(logResults)
          _ <- zlog("Stream completed")
        yield ()) @@ ZIOAspect.tagged(tags.toList*)
      )

  /** The stage that completes the stream until the lifetime service is cancelled.
    */
  private def streamLifetimeGuard = ZPipeline.takeUntil(_ => lifetimeService.cancelled)

  /** Logs the results of the stream.
    */
  private def logResults = ZSink.foreach(result => zlog("Processing completed: %s", result.toString))

/** The companion object for the StreamRunnerServiceImpl class.
  */
object GenericStreamRunnerService:

  /** The required environment for the GenericStreamRunnerService.
    */
  type Environment = StreamLifetimeService & StreamingGraphBuilder & PluginStreamContext & StreamBootstrapper &
    HookManager & MetricTagProvider

  /** Creates a new instance of the GenericStreamRunnerService class.
    *
    * @param builder
    *   The stream graph builder.
    * @param lifetimeService
    *   The stream lifetime service.
    * @return
    *   A new instance of the GenericStreamRunnerService class.
    */
  def apply(
      builder: StreamingGraphBuilder,
      lifetimeService: StreamLifetimeService,
      stagingDataSettings: StagingTableSettings,
      hookManager: HookManager,
      bootstrapper: StreamBootstrapper,
      tagProvider: MetricTagProvider
  ): GenericStreamRunnerService =
    new GenericStreamRunnerService(
      builder,
      lifetimeService,
      stagingDataSettings,
      hookManager,
      bootstrapper,
      tagProvider
    )

  /** The ZLayer for the GenericStreamRunnerService.
    */
  val layer: ZLayer[Environment, Nothing, StreamRunnerService] =
    ZLayer {
      for
        lifetimeService <- ZIO.service[StreamLifetimeService]
        builder         <- ZIO.service[StreamingGraphBuilder]
        context         <- ZIO.service[PluginStreamContext]
        hookManager     <- ZIO.service[HookManager]
        bootstrapper    <- ZIO.service[StreamBootstrapper]
        tagProvider     <- ZIO.service[MetricTagProvider]
      yield GenericStreamRunnerService(
        builder,
        lifetimeService,
        context.staging.table,
        hookManager,
        bootstrapper,
        tagProvider
      )
    }
