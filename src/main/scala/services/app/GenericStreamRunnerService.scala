package com.sneaksanddata.arcane.framework
package services.app

import logging.ZIOLogAnnotations.zlog
import models.settings.StagingDataSettings
import services.app.base.{StreamLifetimeService, StreamRunnerService}
import services.base.TableManager
import services.streaming.base.StreamingGraphBuilder

import zio.stream.{ZPipeline, ZSink}
import zio.{Tag, ZIO, ZLayer}

/**
 * A service that can be used to run a stream.
 *
 * @param builder         The stream graph builder.
 * @param lifetimeService The stream lifetime service.
 */
class GenericStreamRunnerService(builder: StreamingGraphBuilder,
                                 lifetimeService: StreamLifetimeService,
                                 stagingDataSettings: StagingDataSettings,
                                 tableManager: TableManager) extends StreamRunnerService:

  /**
   * Runs the stream.
   *
   * @return A ZIO effect that represents the stream.
   */
  def run: ZIO[Any, Throwable, Unit] =
    lifetimeService.start()
    for
      _ <- zlog("Starting the stream runner")
      _ <- tableManager.createTargetTable
      
      _ <- tableManager.createStagingTable
      _ <- tableManager.clearStagingTable
      
      _ <- tableManager.createBackFillTable
      _ <- tableManager.clearBackFillTable
      
      _ <- builder.produce.via(streamLifetimeGuard).run(logResults)
      _ <- zlog("Stream completed")
    yield ()

  /**
   * The stage that completes the stream until the lifetime service is cancelled.
   */
  private def streamLifetimeGuard = ZPipeline.takeUntil(_ => lifetimeService.cancelled)

  /**
   * Logs the results of the stream.
   */
  private def logResults = ZSink.foreach(result => zlog(s"Processing completed: $result"))

/**
 * The companion object for the StreamRunnerServiceImpl class.
 */
object GenericStreamRunnerService:

  /**
   * The required environment for the GenericStreamRunnerService.
   */
  type Environment = StreamLifetimeService
    & StreamingGraphBuilder
    & StagingDataSettings
    & TableManager

  /**
   * Creates a new instance of the GenericStreamRunnerService class.
   *
   * @param builder         The stream graph builder.
   * @param lifetimeService The stream lifetime service.
   * @return A new instance of the GenericStreamRunnerService class.
   */
  def apply(builder: StreamingGraphBuilder,
            lifetimeService: StreamLifetimeService,
            stagingDataSettings: StagingDataSettings,
            tableManager: TableManager): GenericStreamRunnerService =
    new GenericStreamRunnerService(builder, lifetimeService, stagingDataSettings, tableManager)

  /**
   * The ZLayer for the GenericStreamRunnerService.
   */
  val layer: ZLayer[Environment, Nothing, StreamRunnerService] =
    ZLayer {
      for
        lifetimeService <- ZIO.service[StreamLifetimeService]
        builder <- ZIO.service[StreamingGraphBuilder]
        stagingDataSettings <- ZIO.service[StagingDataSettings]
        tableManager <- ZIO.service[TableManager]
      yield GenericStreamRunnerService(builder, lifetimeService, stagingDataSettings, tableManager)
    }
