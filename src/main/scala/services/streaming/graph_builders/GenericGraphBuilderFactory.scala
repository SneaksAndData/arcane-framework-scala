package com.sneaksanddata.arcane.framework
package services.streaming.graph_builders

import logging.ZIOLogAnnotations.zlog
import models.app.StreamContext
import models.settings.{BackfillBehavior, BackfillSettings}
import services.streaming.base.StreamingGraphBuilder
import services.streaming.graph_builders.backfill.{GenericBackfillMergeGraphBuilder, GenericBackfillOverwriteGraphBuilder}
import services.streaming.graph_builders.base.GenericStreamingGraphBuilder

import zio.{ZIO, ZLayer}

/**
 * A factory that creates a graph builder based on the backfill settings and the stream context.
 */
object GenericGraphBuilderFactory:

  /**
   * The environment required for the graph builder to be created.
   */
  type Environment = GenericBackfillMergeGraphBuilder
    & GenericBackfillOverwriteGraphBuilder
    & GenericStreamingGraphBuilder
    & BackfillSettings
    & StreamContext

  /**
   * The ZLayer for the graph builder injection with runtime dependency resolution.
   */
  val layer: ZLayer[Environment, Nothing, StreamingGraphBuilder] =
      ZLayer.fromZIO(resolveGraphBuilder)

  private def resolveGraphBuilder: ZIO[Environment, Nothing, StreamingGraphBuilder] =
    for backfillSettings <- ZIO.service[BackfillSettings]
        streamContext <- ZIO.service[StreamContext]

        _ <- zlog("resoling graph builder using stream context and backfill settings")
        builder <- (streamContext.IsBackfilling, backfillSettings.backfillBehavior) match
          case (false, _) => ZIO.service[GenericStreamingGraphBuilder]
          case (true, BackfillBehavior.Merge) => ZIO.service[GenericBackfillMergeGraphBuilder]
          case (true, BackfillBehavior.Overwrite) => ZIO.service[GenericBackfillOverwriteGraphBuilder]
        _ <- zlog("Using the stream graph builder: %s", builder.getClass.getName)
    yield builder
