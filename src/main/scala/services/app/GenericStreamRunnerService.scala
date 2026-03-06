package com.sneaksanddata.arcane.framework
package services.app

import logging.ZIOLogAnnotations.zlog
import models.settings.staging.StagingDataSettings
import services.app.base.{StreamLifetimeService, StreamRunnerService}
import services.bootstrap.base.StreamBootstrapper
import services.streaming.base.{HookManager, StreamingGraphBuilder}

import zio.stream.{ZPipeline, ZSink}
import zio.{Tag, ZIO, ZLayer}

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
    stagingDataSettings: StagingDataSettings,
    hookManager: HookManager,
    bootstrapper: StreamBootstrapper
) extends StreamRunnerService:

  /** Runs the stream.
    *
    * @return
    *   A ZIO effect that represents the stream.
    */
  def run: ZIO[Any, Throwable, Unit] =
    lifetimeService.start()
    for
      _ <- zlog("Starting the stream runner")
      _ <- bootstrapper.cleanupStagingTables(
        stagingDataSettings.stagingTablePrefix
      )

      _ <- bootstrapper.createTargetTable
      _ <- bootstrapper.createBackFillTable
      _ <- builder.produce(hookManager).via(streamLifetimeGuard).run(logResults)
      _ <- zlog("Stream completed")
    yield ()

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
  type Environment = StreamLifetimeService & StreamingGraphBuilder & StagingDataSettings & StreamBootstrapper &
    HookManager

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
      stagingDataSettings: StagingDataSettings,
      hookManager: HookManager,
      bootstrapper: StreamBootstrapper
  ): GenericStreamRunnerService =
    new GenericStreamRunnerService(builder, lifetimeService, stagingDataSettings, hookManager, bootstrapper)

  /** The ZLayer for the GenericStreamRunnerService.
    */
  val layer: ZLayer[Environment, Nothing, StreamRunnerService] =
    ZLayer {
      for
        lifetimeService     <- ZIO.service[StreamLifetimeService]
        builder             <- ZIO.service[StreamingGraphBuilder]
        stagingDataSettings <- ZIO.service[StagingDataSettings]
        hookManager         <- ZIO.service[HookManager]
        bootstrapper        <- ZIO.service[StreamBootstrapper]
      yield GenericStreamRunnerService(builder, lifetimeService, stagingDataSettings, hookManager, bootstrapper)
    }
