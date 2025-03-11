package com.sneaksanddata.arcane.framework
package services.app

import logging.ZIOLogAnnotations.zlog
import services.app.base.{StreamLifetimeService, StreamRunnerService}
import services.streaming.base.{MetadataEnrichedRowStreamElement, StreamingGraphBuilder}

import zio.stream.{ZPipeline, ZSink}
import zio.{Tag, ZIO, ZLayer}

/**
 * A service that can be used to run a stream.
 *
 * @param builder The stream graph builder.
 * @param lifetimeService The stream lifetime service.
 */
class GenericStreamRunnerService(builder: StreamingGraphBuilder, lifetimeService: StreamLifetimeService) extends StreamRunnerService:

  /**
   * Runs the stream.
   *
   * @return A ZIO effect that represents the stream.
   */
  def run: ZIO[Any, Throwable, Unit] =
    lifetimeService.start()
    builder.produce.via(streamLifetimeGuard).run(logResults)

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
  type Environment = StreamLifetimeService & StreamingGraphBuilder

  /**
   * Creates a new instance of the GenericStreamRunnerService class.
   *
   * @param builder The stream graph builder.
   * @param lifetimeService The stream lifetime service.
   * @return A new instance of the GenericStreamRunnerService class.
   */
  def apply(builder: StreamingGraphBuilder, lifetimeService: StreamLifetimeService): GenericStreamRunnerService =
    new GenericStreamRunnerService(builder, lifetimeService)

  /**
   * The ZLayer for the GenericStreamRunnerService.
   */
  val layer: ZLayer[Environment, Nothing, StreamRunnerService] =
    ZLayer{
      for
        lifetimeService <- ZIO.service[StreamLifetimeService]
        builder <- ZIO.service[StreamingGraphBuilder]
      yield GenericStreamRunnerService(builder, lifetimeService)
    }
