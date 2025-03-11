package com.sneaksanddata.arcane.framework
package services.app

import logging.ZIOLogAnnotations.zlog
import services.app.base.{StreamLifetimeService, StreamRunnerService}
import services.streaming.base.{MetadataEnrichedRowStreamElement, StreamingGraphBuilder}

import zio.stream.{ZPipeline, ZSink}
import zio.{Tag, ZIO, ZLayer}

class GenericStreamRunnerService(builder: StreamingGraphBuilder, lifetimeService: StreamLifetimeService)
  extends StreamRunnerService:

  def run: ZIO[Any, Throwable, Unit] =
    lifetimeService.start()
    builder.produce.via(streamLifetimeGuard).run(logResults)


  private def streamLifetimeGuard = ZPipeline.takeUntil(_ => lifetimeService.cancelled)

  private def logResults = ZSink.foreach(result => zlog(s"Processing completed: $result"))

object GenericStreamRunnerService:

    type Environment = StreamLifetimeService & StreamingGraphBuilder

    def apply(builder: StreamingGraphBuilder, lifetimeService: StreamLifetimeService): GenericStreamRunnerService =
      new GenericStreamRunnerService(builder, lifetimeService)

    val layer: ZLayer[Environment, Nothing, GenericStreamRunnerService] =
      ZLayer{
        for
          lifetimeService <- ZIO.service[StreamLifetimeService]
          builder <- ZIO.service[StreamingGraphBuilder]
        yield GenericStreamRunnerService(builder, lifetimeService)
      }
