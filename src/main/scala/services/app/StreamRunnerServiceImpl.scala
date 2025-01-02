package com.sneaksanddata.arcane.framework
package services.app

import services.app.base.{StreamLifetimeService, StreamRunnerService}
import services.streaming.base.StreamGraphBuilder

import zio.{ZIO, ZLayer}

/**
 * A service that can be used to run a stream.
 *
 * @param builder The stream graph builder.
 * @param lifetimeService The stream lifetime service.
 */
private class StreamRunnerServiceImpl(builder: StreamGraphBuilder, lifetimeService: StreamLifetimeService) extends StreamRunnerService:

  /**
   * Runs the stream.
   *
   * @return A ZIO effect that represents the stream.
   */
  def run: ZIO[Any, Throwable, Unit] = 
    lifetimeService.start()
    builder.create.run(builder.consume)

/**
 * The companion object for the StreamRunnerServiceImpl class.
 */
object StreamRunnerServiceImpl:

  /**
   * The ZLayer for the stream runner service.
   */
  val layer: ZLayer[StreamGraphBuilder & StreamLifetimeService, Nothing, StreamRunnerService] =
    ZLayer {
      for {
        builder <- ZIO.service[StreamGraphBuilder]
        lifetimeService <- ZIO.service[StreamLifetimeService]
      } yield new StreamRunnerServiceImpl(builder, lifetimeService)
    }
